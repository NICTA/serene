/**
  * Copyright (C) 2015-2016 Data61, Commonwealth Scientific and Industrial Research Organisation (CSIRO).
  * See the LICENCE.txt file distributed with this work for additional
  * information regarding copyright ownership.
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package au.csiro.data61.matcher.matcher.features

import au.csiro.data61.matcher.data._
import au.csiro.data61.matcher.matcher.train.TrainAliases._
import au.csiro.data61.matcher.matcher.train.{ClassImbalanceResampler, TrainingSettings}
import au.csiro.data61.matcher.matcher._
import au.csiro.data61.matcher.nlptools.tokenizer.StringTokenizer
import com.typesafe.scalalogging.LazyLogging

import scala.util._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import scala.language.postfixOps


object FeatureExtractorUtil extends LazyLogging {
  def getFeatureNames(features: List[FeatureExtractor]
                     ): List[String] = {
    features.flatMap {
      case x: SingleFeatureExtractor => List(x.getFeatureName())
      case x: GroupFeatureExtractor => x.getFeatureNames()
    }
  }

  def extractFeatures(attributes: List[DMAttribute],
                      featureExtractors: List[FeatureExtractor]
                     ): List[List[Double]] = {
    // additional preprocessing of attributes (e.g., data type inference, tokenization of column names, etc.)
    val preprocessor = DataPreprocessor()
    //TODO: restore caching?
    // val preprocessedAttributes = attributes.map({rawAttr => (preprocessedAttrCache.getOrElseUpdate(rawAttr.id, preprocessor.preprocess(rawAttr)))})
    val preprocessedAttributes = attributes
      .map({rawAttr =>
        preprocessor.preprocess(rawAttr)})

    logger.info(s"***Extracting features from ${preprocessedAttributes.size} instances...")
    val featuresOfAllInstances = for(i <- 0 until preprocessedAttributes.size) yield {
      val attr = preprocessedAttributes(i)
      if(i % 100 == 0 || (i+1) == preprocessedAttributes.size)
        println("    extracting features from instance " + i + s" of ${preprocessedAttributes.size} : " + attr.rawAttribute.id)
      // val instanceFeatures = featuresCache.getOrElseUpdate(attr.rawAttribute.id, featureExtractors.flatMap({
      val instanceFeatures = featureExtractors.flatMap({
        case fe: SingleFeatureExtractor => List(fe.computeFeature(attr))
        case gfe: GroupFeatureExtractor => gfe.computeFeatures(attr)
      })
      instanceFeatures
    }

    logger.info("***Finished extracting features.")
    featuresOfAllInstances.toList
  }

  def extractTestFeatures(attributes: List[DMAttribute],
                          featureExtractors: List[FeatureExtractor]
                         )(implicit spark: SparkSession): List[List[Double]] = {
    // additional preprocessing of attributes (e.g., data type inference, tokenization of column names, etc.)
    logger.info(s"***Extracting test features with spark from ${attributes.size} instances...")

    // broadcasting big data structures
    val featExtractBroadcast = spark.sparkContext.broadcast(featureExtractors)
    val attrBroadcast = spark.sparkContext.broadcast(attributes)

    val featuresOfAllInstances = Try {
      spark.sparkContext.parallelize(attributes.indices).map {
        idx =>
//          lazy val attr = DataPreprocessor().preprocess(attrBroadcast.value(idx))
//          featExtractBroadcast.value.flatMap {
//            case fe: SingleFeatureExtractor => List(fe.computeFeature(attr))
//            case gfe: GroupFeatureExtractor => gfe.computeFeatures(attr)
//          }
          val attr: SimpleAttribute = getSimpleAttribute(attrBroadcast.value(idx))
          featExtractBroadcast.value.flatMap {
            case fe: SingleFeatureExtractor => List(fe.computeSimpleFeature(attr))
            case gfe: GroupFeatureExtractor => gfe.computeSimpleFeatures(attr)
          }
      }.collect.toList
    } match {
      case Success(testFeatures) =>
        testFeatures
      case Failure(err) =>
        logger.error(s"Failure in extracting test features with spark: $err")
        spark.stop()
        throw new Exception(s"Failure in extracting test features with spark: $err")
    }

    // destroy broadcast variables explicitly
    featExtractBroadcast.destroy()
    attrBroadcast.destroy()

    logger.info("***Finished extracting test features with spark.")
    featuresOfAllInstances
  }

  /**
    * Here we try to push bagging and feature extraction to workers.
    * Needed for prediction.
    * @param attributes
    * @param featureExtractors
    * @param numBags
    * @param bagSize
    * @param spark
    * @return
    */
  def extractBaggingFeatures(attributes: List[DMAttribute],
                             featureExtractors: List[FeatureExtractor],
                             numBags: Int,
                             bagSize: Int
                         )(implicit spark: SparkSession): List[(List[Double], String)] = {
    // additional preprocessing of attributes (e.g., data type inference, tokenization of column names, etc.)
    logger.info(s"***Extracting test features with spark from ${attributes.size} instances...")

    // broadcasting big data structures
    val featExtractBroadcast = spark.sparkContext.broadcast(featureExtractors)
    val attrBroadcast = spark.sparkContext.broadcast(attributes)

    val featuresOfAllInstances = Try {
      spark.sparkContext.parallelize(attributes.indices).flatMap {
        idx => //first extract features from headers, do bagging and only then extract the rest of the features!!!

          val rawAtttr = attrBroadcast.value(idx)
          val groupFeatures: Map[String, List[Double]] = featExtractBroadcast.value.flatMap {
            case gfe: HeaderGroupFeatureExtractor =>
              List((gfe.getGroupName(), gfe.computeSimpleFeatures(getSimpleAttribute(rawAtttr))))
            case _ => None
          }.toMap

          val sampledAttributes: List[SimpleAttribute] = ClassImbalanceResampler.testBaggingAttribute(
            rawAtttr,
            numBags = numBags, bagSize = bagSize
          ).map(getSimpleAttribute)

          sampledAttributes.map {
            attr =>
              val instanceFeatures = featExtractBroadcast.value.flatMap {
                case fe: SingleFeatureExtractor => List(fe.computeSimpleFeature(attr))
                case hgfe: HeaderGroupFeatureExtractor => groupFeatures.getOrElse(hgfe.getGroupName(), List())
                case gfe: GroupFeatureExtractor => gfe.computeSimpleFeatures(attr)
              }
              // we add id of the attribute to keep track to whom the extracted features belong
              (idx.toDouble +: instanceFeatures).toArray
          }
      }.collect.map {
            // we need to convert explicitly to get List[Double] for features and id of the attribute
            row => (row.takeRight(row.length - 1).toList, attributes(row.head.toInt).id)
          }.toList

    } match {
      case Success(testFeatures) =>
        testFeatures
      case Failure(err) =>
        logger.error(s"Failure in extracting test features with spark: $err")
        spark.stop()
        throw new Exception(s"Failure in extracting test features with spark: $err")
    }

    // destroy broadcast variables explicitly
    featExtractBroadcast.destroy()
    attrBroadcast.destroy()

    logger.info("***Finished extracting test features with spark.")
    featuresOfAllInstances
  }

  /**
    * Extract features for training using spark.
    * Here we try to push bagging and feature extraction to workers.
    * @param attributes List of attributes.
    * @param labels Semantic type labels.
    * @param featureExtractors List of feature extractors.
    * @param spark Implicit spark session.
    * @return
    */
  def extractBaggingFeatures(attributes: List[Attribute],
                             labels: SemanticTypeLabels,
                             featureExtractors: List[FeatureExtractor],
                             numBags: Int,
                             bagSize: Int
                            )(implicit spark: SparkSession):List[(List[Double], String)] = {
    logger.info(s"Extracting train features by bagging with spark from ${attributes.size} instances...")
    Try {
      // map from attribute to label
      val attrLabelMap: Map[String, String] = attributes.map {
        attr => (attr.id, labels.findLabel(attr.id))
      }.toMap
      // list of all available labels
      val labelList: List[String] = attrLabelMap.values.toList

      // broadcasting attributes since they are too big
      val attrBroadcast = spark.sparkContext.broadcast(attributes)
      val featExtractBroadcast = spark.sparkContext.broadcast(featureExtractors)

      val answer = spark.sparkContext.parallelize(attributes.indices)
        .flatMap {
          idx =>    //first extract features from headers, do bagging and only then extract the rest of the features!!!
            val rawAtttr = attrBroadcast.value(idx)
            val groupFeatures: Map[String, List[Double]] = featExtractBroadcast.value.flatMap {
              case gfe: HeaderGroupFeatureExtractor =>
                List((gfe.getGroupName(), gfe.computeSimpleFeatures(getSimpleAttribute(rawAtttr))))
              case _ => None
            }.toMap

            // first do bagging and then extract features!
            ClassImbalanceResampler.testBaggingAttribute(
              rawAtttr,
              numBags = numBags, bagSize = bagSize
            ).map(getSimpleAttribute).map {
              attr =>
                val instanceFeatures = featExtractBroadcast.value.flatMap {
                  case fe: SingleFeatureExtractor => List(fe.computeSimpleFeature(attr))
                  case hgfe: HeaderGroupFeatureExtractor => groupFeatures.getOrElse(hgfe.getGroupName(), List())
                  case gfe: GroupFeatureExtractor => gfe.computeSimpleFeatures(attr)
                }
                // we add id of the label to keep track of the class
                (labelList.indexOf(attrLabelMap(attr.attributeName)).toDouble +: instanceFeatures).toArray
            }
        }.collect.map {
        row => (row.takeRight(row.length - 1).toList, labelList(row.head.toInt))
      }.toList
      // destroy broadcast variables explicitly
      featExtractBroadcast.destroy()
      attrBroadcast.destroy()
      answer
    } match {
      case Success(calculation) =>
        logger.info("Finished extracting train features with spark.")
        calculation
      case Failure(err) =>
        logger.error(s"Feature extraction failed: ${err.getMessage}")
        spark.stop()
        throw new Exception(s"Feature extraction failed: ${err.getMessage}")
    }
  }


  def extractFeatures(attributes: List[DMAttribute],
                      labels: SemanticTypeLabels,
                      featureExtractors: List[FeatureExtractor]
                     ): List[(List[Double], String)] = {
    val preprocessor = DataPreprocessor()
    //TODO: restore caching?
    // val preprocessedAttributes = attributes.map({rawAttr => (preprocessedAttrCache.getOrElseUpdate(rawAttr.id, preprocessor.preprocess(rawAttr)))})
    val preprocessedAttributes = attributes.map {
      rawAttr => preprocessor.preprocess(rawAttr)
    }

    logger.info(s"***Extracting features from ${preprocessedAttributes.size} instances...")
    val featuresOfAllInstances = for(i <- 0 until preprocessedAttributes.size) yield {
      val attr = preprocessedAttributes(i)
      if(i % 100 == 0 || (i+1) == preprocessedAttributes.size)
        println("    extracting features from instance " + i + s" of ${preprocessedAttributes.size} : "
          + attr.rawAttribute.id)
      //TODO: restore caching?
      // val instanceFeatures = featuresCache.getOrElseUpdate(attr.rawAttribute.id, featureExtractors.flatMap({
      val instanceFeatures = featureExtractors.flatMap({
        case fe: SingleFeatureExtractor => List(fe.computeFeature(attr))
        case gfe: GroupFeatureExtractor => gfe.computeFeatures(attr)
      })
      (instanceFeatures, labels.findLabel(attr.rawAttribute.id))
    }

    logger.info("Finished extracting features.")
    featuresOfAllInstances.toList
  }


  def extractFeatures(preprocessedAttributes: List[PreprocessedAttribute],
                      labels: SemanticTypeLabels,
                      featureExtractors: List[FeatureExtractor]
                     )(implicit d: DummyImplicit): List[(PreprocessedAttribute, List[Double], String)] = {
    logger.info(s"Extracting features from ${preprocessedAttributes.size} instances...")
    val featuresOfAllInstances = preprocessedAttributes.map {
      attr =>
        val instanceFeatures = featureExtractors.flatMap {
          case fe: SingleFeatureExtractor => List(fe.computeFeature(attr))
          case gfe: GroupFeatureExtractor => gfe.computeFeatures(attr)
        }
        (attr, instanceFeatures, labels.findLabel(attr.rawAttribute.id))
    }

    logger.info("Finished extracting features.")
    featuresOfAllInstances
  }

  /**
    * Helper method to convert list of abstract attributes to list of simple attributes...
    * @param attributes List of abstract attributes
    * @return
    */
  protected def getSimpleAttributes(attributes: List[DMAttribute]): List[SimpleAttribute] = {
    attributes
      .map {
        attr =>
          val attrName = attr.metadata match {
            case Some(meta) => Some(meta.name)
            case _ => None
          }
          SimpleAttribute(attr.id, attrName, attr.values.toArray)
      }
  }

  /**
    * Helper method to convert abstract attribute to simple attribute...
    * @param attribute abstract attribute
    * @return
    */
  protected def getSimpleAttribute(attribute: DMAttribute): SimpleAttribute = {
    val attrName = attribute.metadata match {
      case Some(meta) => Some(meta.name)
      case _ => None
    }
    SimpleAttribute(attribute.id, attrName, attribute.values.toArray)
  }

  /**
    * Extract features for training using spark.
    * @param attributes List of attributes.
    * @param labels Semantic type labels.
    * @param featureExtractors List of feature extractors.
    * @param spark Implicit spark session.
    * @return
    */
  def extractSimpleTrainFeatures(attributes: List[Attribute],
                                 labels: SemanticTypeLabels,
                                 featureExtractors: List[FeatureExtractor]
                                )(implicit spark: SparkSession):List[(List[Double], String)] = {
    logger.info(s"Extracting train features with spark from ${attributes.size} instances...")
    Try {
      // map from attribute to label
      val attrLabelMap: Map[String, String] = attributes.map {
        attr => (attr.id, labels.findLabel(attr.id))
      }.toMap
      // list of all available labels
      val labelList: List[String] = attrLabelMap.values.toList

      // broadcasting attributes since they are too big
      val attrBroadcast = spark.sparkContext.broadcast(attributes)
      val broadcast = spark.sparkContext.broadcast(featureExtractors)

      val answer = spark.sparkContext.parallelize(attributes.indices)
        .map {
          idx =>
            val attr: SimpleAttribute = getSimpleAttribute(attrBroadcast.value(idx))
            val instanceFeatures = broadcast.value.flatMap {
              case fe: SingleFeatureExtractor => List(fe.computeSimpleFeature(attr))
              case gfe: GroupFeatureExtractor => gfe.computeSimpleFeatures(attr)
            }
            (labelList.indexOf(attrLabelMap(attr.attributeName)).toDouble +: instanceFeatures).toArray
        }.collect.map {
        row => (row.takeRight(row.length - 1).toList, labelList(row.head.toInt))
      }.toList
      // destroy broadcast variables explicitly
      broadcast.destroy()
      attrBroadcast.destroy()
      answer
    } match {
      case Success(calculation) =>
        logger.info("Finished extracting train features with spark.")
        calculation
      case Failure(err) =>
        logger.error(s"Feature extraction failed: ${err.getMessage}")
        spark.stop()
        throw new Exception(s"Feature extraction failed: ${err.getMessage}")
    }
  }

  def generateFeatureExtractors(classes: List[String],
                                preprocessedAttributes: List[PreprocessedAttribute],
                                trainingSettings: TrainingSettings,
                                labels: SemanticTypeLabels
                               ): List[FeatureExtractor] = {
    createStandardFeatureExtractors(trainingSettings.featureSettings) ++
      createExampleBasedFeatureExtractors(preprocessedAttributes, labels, classes, trainingSettings.featureSettings)
  }


  def createStandardFeatureExtractors(featureSettings: FeatureSettings): List[FeatureExtractor] = {
    val factoryMethods = List(
      (NumUniqueValuesFeatureExtractor.getFeatureName, NumUniqueValuesFeatureExtractor.apply _),
      (PropUniqueValuesFeatureExtractor.getFeatureName, PropUniqueValuesFeatureExtractor.apply _),
      (PropMissingValuesFeatureExtractor.getFeatureName, PropMissingValuesFeatureExtractor.apply _),
      (NumericalCharRatioFeatureExtractor.getFeatureName, NumericalCharRatioFeatureExtractor.apply _),
      (WhitespaceRatioFeatureExtractor.getFeatureName, WhitespaceRatioFeatureExtractor.apply _),
      (DiscreteTypeFeatureExtractor.getFeatureName, DiscreteTypeFeatureExtractor.apply _),
      (EntropyForDiscreteDataFeatureExtractor.getFeatureName, EntropyForDiscreteDataFeatureExtractor.apply _),
      (PropAlphaCharsFeatureExtractor.getFeatureName, PropAlphaCharsFeatureExtractor.apply _),
      (PropEntriesWithAtSign.getFeatureName, PropEntriesWithAtSign.apply _),
      (PropEntriesWithCurrencySymbol.getFeatureName, PropEntriesWithCurrencySymbol.apply _),
      (PropEntriesWithHyphen.getFeatureName, PropEntriesWithHyphen.apply _),
      (PropEntriesWithParen.getFeatureName, PropEntriesWithParen.apply _),
      (MeanCommasPerEntry.getFeatureName, MeanCommasPerEntry.apply _),
      (MeanForwardSlashesPerEntry.getFeatureName, MeanForwardSlashesPerEntry.apply _),
      (PropRangeFormat.getFeatureName, PropRangeFormat.apply _),
      (DatePatternFeatureExtractor.getFeatureName, DatePatternFeatureExtractor.apply _),
      (DataTypeFeatureExtractor.getGroupName, () => {
        DataTypeFeatureExtractor(
          featureSettings.featureExtractorParams.get(DataTypeFeatureExtractor.getGroupName).flatMap {
            featureParams => featureParams.get("type-map").map {
              case x if featureSettings.rootFilePath.isDefined => featureSettings.rootFilePath.get + "/" + x
              case x => x
            }
          }
        )}
        ),
      (TextStatsFeatureExtractor.getGroupName, TextStatsFeatureExtractor.apply _),
      (NumberTypeStatsFeatureExtractor.getGroupName, NumberTypeStatsFeatureExtractor.apply _),
      (CharDistFeatureExtractor.getGroupName, CharDistFeatureExtractor.apply _),
      (ShannonEntropyFeatureExtractor.getFeatureName, ShannonEntropyFeatureExtractor.apply _)
    )

    //instantiate only those active features
    factoryMethods.filter {
      case (name, factoryMethod) =>
        featureSettings.activeFeatures.contains(name) || featureSettings.activeGroupFeatures.contains(name)
    }.map {
      case (name, factoryMethod) => factoryMethod()
    }
  }

  def createExampleBasedFeatureExtractors(trainingData: List[PreprocessedAttribute],
                                          labels: SemanticTypeLabels,
                                          classes: List[String],
                                          featureSettings: FeatureSettings
                                         ): List[FeatureExtractor] = {
    val attrsWithNames = trainingData.filter(_.rawAttribute.metadata.nonEmpty)

    val factoryMethods = List(
      (RfKnnFeatureExtractor.getGroupName, {
        lazy val auxData = attrsWithNames.map {
          attribute => RfKnnFeature(attribute.rawAttribute.id,
            attribute.rawAttribute.metadata.get.name,
            labels.findLabel(attribute.rawAttribute.id))
        }
        () => {
          val k = featureSettings
            .featureExtractorParams(RfKnnFeatureExtractor.getGroupName)("num-neighbours").toInt
          RfKnnFeatureExtractor(classes, auxData, k)
        }
      }),
      (MinEditDistFromClassExamplesFeatureExtractor.getGroupName, {
        lazy val auxData = attrsWithNames.map {
          attribute =>
            (labels.findLabel(attribute.rawAttribute.id), attribute.rawAttribute.metadata.get.name)
        }
          .groupBy(_._1)
          .map {
            case (className,values) => (className, values.map(_._2))
          }
        () => MinEditDistFromClassExamplesFeatureExtractor(classes, auxData)
      }),
      (MeanCharacterCosineSimilarityFeatureExtractor.getGroupName, {
        lazy val auxData = attrsWithNames.map {
          attribute =>
            (labels.findLabel(attribute.rawAttribute.id), computeCharDistribution(attribute.rawAttribute))
        }
          .groupBy(_._1)
          .map {
            case (className,values) => (className, values.map(_._2))
          }
        () => MeanCharacterCosineSimilarityFeatureExtractor(classes, auxData)
      }),
      (JCNMinWordNetDistFromClassExamplesFeatureExtractor.getGroupName, {
        lazy val auxData = attrsWithNames
          .map{
            attr =>
              val attrId = attr.rawAttribute.id
              val label = labels.findLabel(attrId)
              (label, (attr.preprocessedDataMap("attribute-name-tokenized").asInstanceOf[List[String]]))
          }
          .groupBy(_._1)
          .map {
            case (className, values) => (className, values.map(_._2))
          }
        () => {
          val maxComparisons = featureSettings
            .featureExtractorParams(
              JCNMinWordNetDistFromClassExamplesFeatureExtractor.getGroupName)("max-comparisons-per-class").toInt
          JCNMinWordNetDistFromClassExamplesFeatureExtractor(classes, auxData, maxComparisons)
        }
      }),
      (LINMinWordNetDistFromClassExamplesFeatureExtractor.getGroupName, {
        lazy val auxData = attrsWithNames.map {
          attr =>
            val attrId = attr.rawAttribute.id
            val label = labels.findLabel(attrId)
            (label, attr.preprocessedDataMap("attribute-name-tokenized").asInstanceOf[List[String]])
        }.groupBy(_._1)
          .map {
            case (className, values) => (className, values.map({_._2}))
          }
        () => {
          val maxComparisons = featureSettings
            .featureExtractorParams(LINMinWordNetDistFromClassExamplesFeatureExtractor.getGroupName)(
              "max-comparisons-per-class").toInt
          LINMinWordNetDistFromClassExamplesFeatureExtractor(classes, auxData, maxComparisons)
        }
      })
    )

    //instantiate only those active features
    factoryMethods.filter({case (name, factoryMethod) =>
      featureSettings.activeGroupFeatures.contains(name)
    }).map({case (name, factoryMethod) => factoryMethod()}).toList
  }

  def generateSimpleFeatureExtractors(classes: List[String],
                                      preprocessedAttributes: List[DMAttribute],
                                      trainingSettings: TrainingSettings,
                                      labels: SemanticTypeLabels
                                     ): List[FeatureExtractor] = {
    createStandardFeatureExtractors(trainingSettings.featureSettings) ++
      createSimpleExampleBasedFeatureExtractors(preprocessedAttributes, labels, classes, trainingSettings.featureSettings)
  }


  def createSimpleExampleBasedFeatureExtractors(trainingData: List[DMAttribute],
                                                labels: SemanticTypeLabels,
                                                classes: List[String],
                                                featureSettings: FeatureSettings
                                               ): List[FeatureExtractor] = {

    val newAttrs: List[SimpleAttribute] = getSimpleAttributes(trainingData)

    val attrsWithNames = newAttrs.filter(_.metaName.isDefined)

    val factoryMethods = List(
      (RfKnnFeatureExtractor.getGroupName, {
        lazy val auxData = attrsWithNames.map {
          attribute => RfKnnFeature(attribute.attributeName,
            attribute.metaName.get,
            labels.findLabel(attribute.attributeName))
        }
        () => {
          val k = featureSettings
            .featureExtractorParams(RfKnnFeatureExtractor.getGroupName)("num-neighbours").toInt
          RfKnnFeatureExtractor(classes, auxData, k)
        }
      }),
      (MinEditDistFromClassExamplesFeatureExtractor.getGroupName, {
        lazy val auxData = attrsWithNames.map {
          attribute =>
            (labels.findLabel(attribute.attributeName), attribute.metaName.get)
        }
          .groupBy(_._1)
          .map {
            case (className,values) => (className, values.map(_._2))
          }
        () => MinEditDistFromClassExamplesFeatureExtractor(classes, auxData)
      }),
      (MeanCharacterCosineSimilarityFeatureExtractor.getGroupName, {
        lazy val auxData = attrsWithNames.map {
          attribute =>
            (labels.findLabel(attribute.attributeName), attribute.charDist)
        }
          .groupBy(_._1)
          .map {
            case (className,values) => (className, values.map(_._2))
          }
        () => MeanCharacterCosineSimilarityFeatureExtractor(classes, auxData)
      }),
      (JCNMinWordNetDistFromClassExamplesFeatureExtractor.getGroupName, {
        lazy val auxData = attrsWithNames
          .map{
            attr =>
              val attrId = attr.attributeName
              val label = labels.findLabel(attrId)
              (label, attr.attributeNameTokenized)
          }
          .groupBy(_._1)
          .map {
            case (className, values) => (className, values.map(_._2))
          }
        () => {
          val maxComparisons = featureSettings
            .featureExtractorParams(
              JCNMinWordNetDistFromClassExamplesFeatureExtractor.getGroupName)("max-comparisons-per-class").toInt
          JCNMinWordNetDistFromClassExamplesFeatureExtractor(classes, auxData, maxComparisons)
        }
      }),
      (LINMinWordNetDistFromClassExamplesFeatureExtractor.getGroupName, {
        lazy val auxData = attrsWithNames.map {
          attr =>
            val attrId = attr.attributeName
            val label = labels.findLabel(attrId)
            (label, attr.attributeNameTokenized)
        }.groupBy(_._1)
          .map {
            case (className, values) => (className, values.map({_._2}))
          }
        () => {
          val maxComparisons = featureSettings
            .featureExtractorParams(LINMinWordNetDistFromClassExamplesFeatureExtractor.getGroupName)(
              "max-comparisons-per-class").toInt
          LINMinWordNetDistFromClassExamplesFeatureExtractor(classes, auxData, maxComparisons)
        }
      })
    )
    //instantiate only those active features
    factoryMethods.filter({case (name, factoryMethod) =>
      featureSettings.activeGroupFeatures.contains(name)
    }).map({case (name, factoryMethod) => factoryMethod()})
  }


  def computeTokens(attribute: DMAttribute): List[String] = {
    val nameRegex = "([^@]+)@(.+)".r
    attribute match {
      case (Attribute(_, Some(Metadata(name,_)), _, _)) => {
        name match {
          case nameRegex(name, _) => StringTokenizer.tokenize(name)
          case x => StringTokenizer.tokenize(x)
        }
      }
      case _ => List()
    }
  }

  def computeCharDistribution(attribute: DMAttribute
                             ): Map[Char,Double] = {
    val counts: Map[Char,Int] = attribute.values
      .flatMap(_.toCharArray)
      .groupBy(_.toChar)
      .mapValues(_.size)

    if(counts.nonEmpty) {
      //we downscale the counts vector by 1/maxCount, compute the norm, then upscale by maxCount to prevent overflow
      val maxCount = counts.values.max
      val dscaled = counts.values
        .map(_.toDouble/maxCount.toDouble) // normalize counts
        .foldLeft(0.0)(_ + Math.pow(_, 2)) // compute sum of squares

      val norm = Math.sqrt(dscaled) * maxCount // the norm of the counts
      val normCf = norm match {
        case 0 => counts.map { case (x, _) => (x,0.0) }
        case normbig =>
          counts.map { case (x, y) => (x, y.toDouble / normbig) }
      }

      //      val length = Math.abs(Math.sqrt(normCf.values.map({x=>x*x}).sum) - 1.0)
      //      assert(Math.abs(Math.sqrt(normCf.values.map({x=>x*x}).sum) - 1.0) <= 0.00005,
      //        "length of char freq vector is " + length
      //          + "\nnorm: " + norm
      //          + "\ncounts: " + counts
      //          + "\ncounts^2: " + (counts.values.map({case x => x*x})))
      normCf
    } else {
      Map()
    }
  }

  def printFeatureExtractorNames(featureExtractors: List[FeatureExtractor]) = {
    logger.info("Features Used:\n\t")
    logger.info(featureExtractors.map {
      case fe: SingleFeatureExtractor => fe.getFeatureName
      case gfe: GroupFeatureExtractor => gfe.getGroupName
    }.mkString("\n\t"))
  }
}