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
import au.csiro.data61.matcher.matcher.train.TrainingSettings
import au.csiro.data61.matcher.matcher._
import com.typesafe.scalalogging.LazyLogging

import scala.util._
import scala.io._
import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, Encoders, Row, SparkSession}

import scala.language.postfixOps
import com.esotericsoftware.kryo.Kryo

import scala.collection.mutable

trait FeatureExtractor

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
                         )(implicit sc: SparkContext): List[List[Double]] = {
    logger.info(s"***Preprocessing test features ${attributes.size} instances...")
    // additional preprocessing of attributes (e.g., data type inference, tokenization of column names, etc.)
    val preprocessor = DataPreprocessor()
    val preprocessedAttributes = attributes.map(preprocessor.preprocess)

    //    val preprocessedAttributes = Try {
    //      sc.parallelize(attributes)
    //        .map(preprocessor.preprocess).collect.toList
    //    } match {
    //      case Success(procAttrs) =>
    //        procAttrs
    //      case Failure(err) =>
    //        logger.error(s"Failure in preprocessing test features with spark: $err")
    //        sc.stop()
    //        throw new Exception(s"Failure in preprocessing test features with spark: $err")
    //    }
    logger.info(s"***Extracting test features with spark from ${preprocessedAttributes.size} instances...")

    val featuresOfAllInstances = Try {
      val featExtractBroadcast = sc.broadcast(featureExtractors)
      sc.parallelize(preprocessedAttributes).map {
        attr =>
          featExtractBroadcast.value.flatMap {
            case fe: SingleFeatureExtractor => List(fe.computeFeature(attr))
            case gfe: GroupFeatureExtractor => gfe.computeFeatures(attr)
          }
      }.collect.toList
    } match {
      case Success(testFeatures) =>
        testFeatures
      case Failure(err) =>
        logger.error(s"Failure in extracting test features with spark: $err")
        sc.stop()
        throw new Exception(s"Failure in extracting test features with spark: $err")
    }

    logger.info("***Finished extracting test features with spark.")
    featuresOfAllInstances
  }


  def extractFeatures(attributes: List[DMAttribute],
                      labels: SemanticTypeLabels,
                      featureExtractors: List[FeatureExtractor]
                     ): List[(PreprocessedAttribute, List[Any], String)] = {
    val preprocessor = DataPreprocessor()
    //TODO: restore caching?
    // val preprocessedAttributes = attributes.map({rawAttr => (preprocessedAttrCache.getOrElseUpdate(rawAttr.id, preprocessor.preprocess(rawAttr)))})
    val preprocessedAttributes = attributes.map({rawAttr => preprocessor.preprocess(rawAttr)})

    logger.info(s"***Extracting features from ${preprocessedAttributes.size} instances...")
    val featuresOfAllInstances = for(i <- 0 until preprocessedAttributes.size) yield {
      val attr = preprocessedAttributes(i)
      if(i % 100 == 0 || (i+1) == preprocessedAttributes.size) println("    extracting features from instance " + i + s" of ${preprocessedAttributes.size} : " + attr.rawAttribute.id)
      //TODO: restore caching?
      // val instanceFeatures = featuresCache.getOrElseUpdate(attr.rawAttribute.id, featureExtractors.flatMap({
      val instanceFeatures = featureExtractors.flatMap({
        case fe: SingleFeatureExtractor => List(fe.computeFeature(attr))
        case gfe: GroupFeatureExtractor => gfe.computeFeatures(attr)
      })
      (attr, instanceFeatures, labels.findLabel(attr.rawAttribute.id))
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
    * Split feature extractors into groups according to how much input is needed to calculate the feature.
    * Instead of using the preprocessedAttribute completely we create smaller strongly-typed data structures
    * which are better to handle for spark2.1.
    * @param featureExtractors List of specified feature extractors.
    * @return A map of grouped feature extractors.
    */
  def splitFeatureExtractors(featureExtractors: List[FeatureExtractor]
                            ): Map[String, List[FeatureExtractor]] = {
    logger.info("Splitting feature extractors")
    featureExtractors.flatMap {
      case f: SingleFeatureValuesExtractor => List(("SingleFeatureValuesExtractor", f))
      case f: SingleFeatureExtractor => List(("SingleFeatureExtractor", f))
      case gf: GroupFeatureFullExtractor => List(("GroupFeatureFullExtractor", gf))
      case gf: GroupFeatureLimExtractor => List(("GroupFeatureLimExtractor", gf))
      case gf: GroupFeatureExtractor => List(("GroupFeatureExtractor", gf))
      case _ => None
    }.groupBy(_._1)
      .mapValues(_.map(_._2))
  }

  /**
    * Per each group of feature extractors we need to compute features by creating a smaller data structure from
    * preprocessedAttributes. We use spark here.
    * @param preprocessedAttributes List of preprocessed attributes.
    * @param sfe List of feature extractors which need only values from the attribute and which compute a single double.
    * @param spark Implicit spark session.
    * @return List of generated attribute id and corresponding computed single features.
    */
  def computeSingleFeatureValues(preprocessedAttributes: List[PreprocessedAttribute],
                                 sfe: List[SingleFeatureValuesExtractor])
                                (implicit spark: SparkSession): List[(Int,List[Double])] = {
    logger.info(s"computeSingleFeatureValues: ${sfe.size}")
    // this will make implicit conversion to spark dataset work
    import spark.implicits._
    // custom kryo serializers for our custom classes
    import scala.reflect.ClassTag
    implicit def kryoEncoder[A](implicit ct: ClassTag[A]) =
      org.apache.spark.sql.Encoders.kryo[A](ct)

    val newAttrs: List[PreprocessedValues] = preprocessedAttributes
      .zipWithIndex.map {
      case (prepAttr, attrID) => PreprocessedValues(attrID,
        prepAttr.rawAttribute.values.toArray)
    }

//    spark.sparkContext.parallelize(newAttrs).map {
//      attr =>
//        (attr.attributeID, sfe.map(_.computeFeatureValues(attr)))
//    }.collect.toList
    newAttrs.toDS.map {
      attr =>
        (attr.attributeID.toDouble +: sfe.map(_.computeFeatureValues(attr))).toArray
    }.collect.map {
      row => (row(0).toInt, row.takeRight(row.length - 1).toList)
    }
      .toList
  }

  /**
    * We compute single features on the preprocesedAttrbibutes. We don't use spark.
    * @param preprocessedAttributes List of preprocessed attributes.
    * @param sfe List of feature extractors which compute a single double for the preprocessedAttribute.
    * @param spark Implicit spark session.
    * @return List of generated attribute id and corresponding computed single features.
    */
  def computeSingleFeatures(preprocessedAttributes: List[PreprocessedAttribute],
                                 sfe: List[SingleFeatureExtractor])
                                (implicit spark: SparkSession): List[(Int,List[Double])] = {
    logger.info(s"computeSingleFeatures: ${sfe.size}")
    preprocessedAttributes.zipWithIndex.map {
      case (attr: PreprocessedAttribute, attrID: Int) =>
        (attrID, sfe.map(_.computeFeature(attr)))
    }
  }

  /**
    * Per each group of feature extractors we need to compute features by creating a smaller data structure from
    * preprocessedAttributes. We don't use spark here.
    * @param preprocessedAttributes List of preprocessed attributes.
    * @param gfe List of feature extractors which compute list of doubles for the preprocessedAttribute.
    * @param spark Implicit spark session.
    * @return List of generated attribute id and corresponding computed group features.
    */
  def computeGroupFeatures(preprocessedAttributes: List[PreprocessedAttribute],
                           gfe: List[GroupFeatureExtractor])
                           (implicit spark: SparkSession): List[(Int,List[Double])] = {
    logger.info(s"computeGroupFeatures: ${gfe.size}")
    preprocessedAttributes.zipWithIndex.map {
      case (attr: PreprocessedAttribute, attrID: Int) =>
        (attrID, gfe.flatMap(_.computeFeatures(attr)))
    }
  }

  /**
    * Per each group of feature extractors we need to compute features by creating a smaller data structure
    * from preprocessedAttributes. We use spark here.
    * @param preprocessedAttributes List of preprocessed attributes.
    * @param gfe List of feature extractors which compute list of doubles and need values of the attribute
    *            with parts of preprocessedDataMap.
    * @param spark Implicit spark session.
    * @return List of generated attribute id and corresponding computed group features.
    */
  def computeGroupFeatureFull(preprocessedAttributes: List[PreprocessedAttribute],
                              gfe: List[GroupFeatureFullExtractor])
                             (implicit spark: SparkSession): List[(Int,List[Double])] = {
    logger.info(s"computeGroupFeatureFull: ${gfe.size}")
    // this will make implicit conversion to spark dataset work
    import spark.implicits._
    // custom kryo serializers for our custom classes
    import scala.reflect.ClassTag
    implicit def kryoEncoder[A](implicit ct: ClassTag[A]) =
      org.apache.spark.sql.Encoders.kryo[A](ct)

    val newAttrs: List[FullPreprocessedAttribute] = preprocessedAttributes
      .zipWithIndex.map {
      case (prepAttr, attrID) =>
        val attrName = prepAttr.rawAttribute.metadata match {
          case Some(meta) => Some(meta.name)
          case _ => None
        }
        val charDist = prepAttr.preprocessedDataMap.getOrElse("normalised-char-frequency-vector", Map())
          .asInstanceOf[Map[Char,Double]]
        val attributeNameTokenized: List[String] = prepAttr
          .preprocessedDataMap.getOrElse("attribute-name-tokenized", List())
          .asInstanceOf[List[String]]
        val inferredMap: Map[String, Boolean] = prepAttr
          .preprocessedDataMap.filterKeys(Set("inferred-type-float",
          "inferred-type-integer", "inferred-type-long",
          "inferred-type-boolean", "inferred-type-date",
          "inferred-type-time", "inferred-type-datetime",
          "inferred-type-string"))
          .asInstanceOf[Map[String,Boolean]]
          .map(identity) // there's a weird bug in spark, we need this map so that serialization is ok

        FullPreprocessedAttribute(attrID, prepAttr.rawAttribute.id, attrName,
          prepAttr.rawAttribute.values.toArray, attributeNameTokenized, charDist, inferredMap)
    }

    val broadcast = spark.sparkContext.broadcast(gfe)

    // we can't use spark dataset since preprocessedDataMap contains scala.Any
    spark.sparkContext.parallelize(newAttrs).map {
      attr =>
        (attr.attributeID, broadcast.value.flatMap(_.computeFeatureFull(attr)))
    }.collect.toList
  }

  /**
    * Per each group of feature extractors we need to compute features by creating a smaller data structure
    * from preprocessedAttributes. We use spark here.
    * @param preprocessedAttributes List of preprocessed attributes.
    * @param gfe List of feature extractors which compute list of doubles and need only parts of preprocessedDataMap.
    * @param spark Implicit spark session.
    * @return List of generated attribute id and corresponding computed group features.
    */
  def computeGroupFeatureLim(preprocessedAttributes: List[PreprocessedAttribute],
                              gfe: List[GroupFeatureLimExtractor])
                             (implicit spark: SparkSession): List[(Int,List[Double])] = {
    logger.info(s"computeGroupFeatureLim: ${gfe.size}")
    // this will make implicit conversion to spark dataset work
    import spark.implicits._
    // custom kryo serializers for our custom classes
    import scala.reflect.ClassTag
    implicit def kryoEncoder[A](implicit ct: ClassTag[A]) =
      org.apache.spark.sql.Encoders.kryo[A](ct)

    val newAttrs: List[LimPreprocessedAttribute] = preprocessedAttributes
      .zipWithIndex.map {
      case (prepAttr, attrID) =>
        val attrName = prepAttr.rawAttribute.metadata match {
          case Some(meta) => Some(meta.name)
          case _ => None
        }
        val charDist = prepAttr.preprocessedDataMap.getOrElse("normalised-char-frequency-vector", Map())
          .asInstanceOf[Map[Char,Double]]
        val attributeNameTokenized: List[String] = prepAttr
          .preprocessedDataMap.getOrElse("attribute-name-tokenized", List())
          .asInstanceOf[List[String]]
        val inferredMap: Map[String, Boolean] = prepAttr
          .preprocessedDataMap.filterKeys(Set("inferred-type-float",
          "inferred-type-integer", "inferred-type-long",
          "inferred-type-boolean", "inferred-type-date",
          "inferred-type-time", "inferred-type-datetime",
          "inferred-type-string"))
          .asInstanceOf[Map[String,Boolean]]
          .map(identity) // there's a weird bug in spark, we need this map so that serialization is ok

        LimPreprocessedAttribute(attrID, prepAttr.rawAttribute.id, attrName,
          attributeNameTokenized, charDist, inferredMap)
    }

    val broadcast = spark.sparkContext.broadcast(gfe)

    // we can't use spark dataset since preprocessedDataMap contains scala.Any and scala.Char
    spark.sparkContext.parallelize(newAttrs).map {
      attr =>
        (attr.attributeID, broadcast.value.flatMap(_.computeFeaturesLim(attr)))
    }.collect.toList
  }

  /**
    * Per each group of feature extractors we need to compute features by creating a smaller data structure
    * from preprocessedAttributes. We use spark here.
    * @param preprocessedAttributes List of preprocessed attributes.
    * @param featureExtractors List of all features to be extracted.
    * @param spark Implicit spark session.
    * @return List of generated attribute id and corresponding computed group features.
    */
  def computeAllFeatures(preprocessedAttributes: List[PreprocessedAttribute],
                         featureExtractors: List[FeatureExtractor])
                        (implicit spark: SparkSession)
  : (List[(Int,List[Double])], List[String]) = {
    logger.info(s"Computing features: : ${featureExtractors.size}")
    val splitted: Map[String, List[FeatureExtractor]] = splitFeatureExtractors(featureExtractors)

//    splitted.map{
//      case ("SingleFeatureValuesExtractor", sfe: List[SingleFeatureValuesExtractor]) =>
//        (computeSingleFeatureValues(preprocessedAttributes, sfe),
//          sfe.map(_.getFeatureName()))
//
//    }

    val f1: (List[(Int,List[Double])], List[String]) =  splitted.get("SingleFeatureValuesExtractor") match{
      case Some(sfe: List[SingleFeatureValuesExtractor]) =>
        (computeSingleFeatureValues(preprocessedAttributes, sfe),
          sfe.map(_.getFeatureName()))
      case None => (List(), List())
    }
    logger.info(s"SingleFeatureValuesExtractor  success")

    val f2: (List[(Int,List[Double])], List[String]) =  splitted.get("SingleFeatureExtractor") match {
      case Some(sfe: List[SingleFeatureExtractor]) =>
        (computeSingleFeatures(preprocessedAttributes, sfe),
          sfe.map(_.getFeatureName()))
      case None => (List(), List())
    }
    logger.info(s"SingleFeatureExtractor  success")

    val f3: (List[(Int,List[Double])], List[String])=  splitted.get("GroupFeatureFullExtractor") match {
      case Some( gfe: List[GroupFeatureFullExtractor]) =>
        (computeGroupFeatureFull(preprocessedAttributes, gfe),
          gfe.flatMap(_.getFeatureNames()))
      case None => (List(), List())
    }
    logger.info(s"GroupFeatureFullExtractor  success")

    val f4: (List[(Int,List[Double])], List[String])=  splitted.get("GroupFeatureLimExtractor") match {
      case Some(gfe: List[GroupFeatureLimExtractor]) =>
        (computeGroupFeatureLim(preprocessedAttributes, gfe),
          gfe.flatMap(_.getFeatureNames()))
      case None => (List(), List())
    }
    logger.info(s"GroupFeatureLimExtractor  success")

    val f5: (List[(Int,List[Double])], List[String]) =  splitted.get("GroupFeatureExtractor") match {
      case Some(gfe: List[GroupFeatureExtractor]) =>
        (computeGroupFeatures(preprocessedAttributes, gfe),
          gfe.flatMap(_.getFeatureNames()))
      case None => (List(), List())
    }
    logger.info(s"GroupFeatureExtractor  success")

    val temp: List[(Int,List[Double])] = f1._1 ::: f2._1 ::: f3._1 ::: f4._1 ::: f5._1
    val featureNames: List[String] = f1._2 ::: f2._2 ::: f3._2 ::: f4._2 ::: f5._2

    logger.info(s"FeatureExtractors concatenated")

    (temp.groupBy(_._1)
      .mapValues {
        vals => vals.flatMap {
          case (i: Int, lists: List[Double]) => lists
        }
      }.toList, featureNames)
  }

  /**
    * Extract features for training using spark.
    * @param preprocessedAttributes List of preprocessed attributes.
    * @param labels Smenatic type labels.
    * @param featureExtractors List of feature extractors.
    * @param spark Implicit spark session.
    * @return
    */
  def extractTrainFeatures(preprocessedAttributes: List[PreprocessedAttribute],
                           labels: SemanticTypeLabels,
                           featureExtractors: List[FeatureExtractor]
                          )(implicit spark: SparkSession)
  : (List[(PreprocessedAttribute, List[Double], String)], List[String]) = {
    logger.info(s"Extracting features with spark from ${preprocessedAttributes.size} instances...")
    Try {

      // we create unique ids per each preprocessed attribute
      // we cannot use rawAttribute.id since after resampling it's not unique any more
      val (featuresExtracted: List[(Int,List[Double])], featureNames: List[String]) =
        computeAllFeatures(preprocessedAttributes, featureExtractors)
      logger.info("Converting features...")
      (featuresExtracted.map {
        case (attrID, instanceFeatures) =>
          val attr = preprocessedAttributes(attrID)
          (attr, instanceFeatures, labels.findLabel(attr.rawAttribute.id))
      }, featureNames)
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
          featureSettings.featureExtractorParams.get(DataTypeFeatureExtractor.getGroupName).flatMap({
            case featureParams => featureParams.get("type-map").map({
              case x if !featureSettings.rootFilePath.isEmpty => featureSettings.rootFilePath.get + "/" + x
              case x => x
            })
          })
        )}
        ),
      (TextStatsFeatureExtractor.getGroupName, TextStatsFeatureExtractor.apply _),
      (NumberTypeStatsFeatureExtractor.getGroupName, NumberTypeStatsFeatureExtractor.apply _),
      (CharDistFeatureExtractor.getGroupName, CharDistFeatureExtractor.apply _)
    )

    //instantiate only those active features
    factoryMethods.filter({case (name, factoryMethod) =>
      (featureSettings.activeFeatures.contains(name) || featureSettings.activeGroupFeatures.contains(name))
    }).map({case (name, factoryMethod) => factoryMethod()}).toList
  }

  def createExampleBasedFeatureExtractors(trainingData: List[PreprocessedAttribute],
                                          labels: SemanticTypeLabels,
                                          classes: List[String],
                                          featureSettings: FeatureSettings
                                         ): List[FeatureExtractor] = {
    val attrsWithNames = trainingData.filter(_.rawAttribute.metadata.nonEmpty)

    val factoryMethods = List(
      (RfKnnFeatureExtractor.getGroupName, {
        lazy val auxData = attrsWithNames.map{
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

trait SingleFeatureExtractor extends FeatureExtractor {
  def getFeatureName(): String
  def computeFeature(attribute: PreprocessedAttribute): Double
}

trait SingleFeatureValuesExtractor extends SingleFeatureExtractor {
  def getFeatureName(): String
  def computeFeature(attribute: PreprocessedAttribute): Double
  def computeFeatureValues(attr: PreprocessedValues): Double
}

trait GroupFeatureExtractor extends FeatureExtractor {
  def getGroupName(): String
  def getFeatureNames(): List[String]
  def computeFeatures(attribute: PreprocessedAttribute): List[Double]
}

trait GroupFeatureLimExtractor extends GroupFeatureExtractor {
  def getGroupName(): String
  def getFeatureNames(): List[String]
  def computeFeatures(attribute: PreprocessedAttribute): List[Double]
  def computeFeaturesLim(attr: LimPreprocessedAttribute): List[Double]
}

trait GroupFeatureFullExtractor extends GroupFeatureExtractor {
  def getGroupName(): String
  def getFeatureNames(): List[String]
  def computeFeatures(attribute: PreprocessedAttribute): List[Double]
  def computeFeatureFull(attr: FullPreprocessedAttribute): List[Double]
}

object NumUniqueValuesFeatureExtractor {
  def getFeatureName() = "num-unique-vals"
}
case class NumUniqueValuesFeatureExtractor() extends SingleFeatureValuesExtractor {
  override def getFeatureName(): String = NumUniqueValuesFeatureExtractor.getFeatureName

  override def computeFeature(attribute: PreprocessedAttribute): Double = {
    attribute.rawAttribute.values.map(_.toLowerCase.trim).distinct.size
  }

  override def computeFeatureValues(attr: PreprocessedValues): Double = {
    attr.values.map(_.toLowerCase.trim).distinct.length
  }
}


object PropUniqueValuesFeatureExtractor {
  def getFeatureName() = "prop-unique-vals"
}
case class PropUniqueValuesFeatureExtractor() extends SingleFeatureValuesExtractor {
  override def getFeatureName(): String = PropUniqueValuesFeatureExtractor.getFeatureName

  override def computeFeature(attribute: PreprocessedAttribute): Double = {
    val values = attribute.rawAttribute.values
    values.map(_.toLowerCase.trim).distinct.size.toDouble / values.size
  }

  override def computeFeatureValues(attr: PreprocessedValues): Double = {
    attr.values.map(_.toLowerCase.trim).distinct.length.toDouble / attr.values.length
  }
}


object PropMissingValuesFeatureExtractor {
  def getFeatureName(): String = "prop-missing-vals"
}
case class PropMissingValuesFeatureExtractor() extends SingleFeatureValuesExtractor {
  override def getFeatureName(): String = PropMissingValuesFeatureExtractor.getFeatureName

  override def computeFeature(attribute: PreprocessedAttribute): Double = {
    val values = attribute.rawAttribute.values
    values.count(_.trim.length == 0).toDouble / values.size
  }

  override def computeFeatureValues(attr: PreprocessedValues): Double = {
    attr.values.count(_.trim.length == 0).toDouble / attr.values.length
  }
}


object PropAlphaCharsFeatureExtractor {
  def getFeatureName(): String = "ratio-alpha-chars"
}
case class PropAlphaCharsFeatureExtractor() extends SingleFeatureValuesExtractor {
  override def getFeatureName(): String = PropAlphaCharsFeatureExtractor.getFeatureName

  override def computeFeature(attribute: PreprocessedAttribute): Double = {
    val attrContent = attribute.rawAttribute.values.mkString("")
    if(attrContent.nonEmpty) {
      attrContent.replaceAll("[^a-zA-Z]","").length.toDouble / attrContent.length
    } else {
      -1.0
    }
  }

  override def computeFeatureValues(attr: PreprocessedValues): Double = {
    val attrContent = attr.values.mkString("")
    if(attrContent.nonEmpty) {
      attrContent.replaceAll("[^a-zA-Z]","").length.toDouble / attrContent.length
    } else {
      -1.0
    }
  }
}


object PropEntriesWithAtSign {
  def getFeatureName(): String = "prop-entries-with-at-sign"
}
case class PropEntriesWithAtSign() extends SingleFeatureValuesExtractor {
  override def getFeatureName(): String = PropEntriesWithAtSign.getFeatureName

  override def computeFeature(attribute: PreprocessedAttribute): Double = {
    val attrContent = attribute.rawAttribute.values.filter { x: String => x.nonEmpty }
    if(attrContent.nonEmpty) {
      attrContent.count{
        x:String => x.contains("@")
      }.toDouble / attrContent.size
    } else {
      -1.0
    }
  }

  override def computeFeatureValues(attr: PreprocessedValues): Double = {
    val attrContent = attr.values.filter(_.nonEmpty)
    if(attrContent.nonEmpty) {
      attrContent.count(_.contains("@")).toDouble / attrContent.length
    } else {
      -1.0
    }
  }

}


object PropEntriesWithCurrencySymbol {
  def getFeatureName() = "prop-entries-with-currency-symbol"
}
case class PropEntriesWithCurrencySymbol() extends SingleFeatureValuesExtractor {
  val currencySymbols = List("$","AUD")

  override def getFeatureName(): String = PropEntriesWithCurrencySymbol.getFeatureName

  override def computeFeature(attribute: PreprocessedAttribute): Double = {
    val attrContent = attribute.rawAttribute.values.filter { x: String => x.nonEmpty}
    if(attrContent.nonEmpty) {
      attrContent.count{
        x: String => currencySymbols.exists(x.contains)
      }.toDouble / attrContent.size
    } else {
      -1.0
    }
  }

  override def computeFeatureValues(attr: PreprocessedValues): Double = {
    val attrContent = attr.values.filter(_.nonEmpty)
    if(attrContent.nonEmpty) {
      attrContent.count{
        x: String => currencySymbols.exists(x.contains)
      }.toDouble / attrContent.size
    } else {
      -1.0
    }
  }
}


object PropEntriesWithHyphen {
  def getFeatureName(): String = "prop-entries-with-hyphen"
}
case class PropEntriesWithHyphen() extends SingleFeatureValuesExtractor {
  override def getFeatureName(): String = PropEntriesWithHyphen.getFeatureName

  override def computeFeature(attribute: PreprocessedAttribute): Double = {
    val attrContent = attribute.rawAttribute.values.filter { x: String => x.nonEmpty}
    if(attrContent.nonEmpty) {
      attrContent.count{
        x: String => x.contains("-")
      }.toDouble / attrContent.size
    } else {
      -1.0
    }
  }

  override def computeFeatureValues(attr: PreprocessedValues): Double = {
    val attrContent = attr.values.filter(_.nonEmpty)
    if(attrContent.nonEmpty) {
      attrContent.count(_.contains("-")).toDouble / attrContent.size
    } else {
      -1.0
    }
  }
}

object PropEntriesWithParen {
  def getFeatureName(): String = "prop-entries-with-paren"
}
case class PropEntriesWithParen() extends SingleFeatureValuesExtractor {
  override def getFeatureName(): String = PropEntriesWithParen.getFeatureName

  override def computeFeature(attribute: PreprocessedAttribute): Double = {
    val attrContent = attribute.rawAttribute.values.filter { x: String => x.nonEmpty }
    if(attrContent.nonEmpty) {
      attrContent.count {
        x: String => x.contains("(") || x.contains(")")
      }.toDouble / attrContent.size
    } else {
      -1.0
    }
  }

  override def computeFeatureValues(attr: PreprocessedValues): Double = {
    val attrContent = attr.values.filter(_.nonEmpty)
    if(attrContent.nonEmpty) {
      attrContent.count {
        x: String => x.contains("(") || x.contains(")")
      }.toDouble / attrContent.size
    } else {
      -1.0
    }
  }
}

object MeanCommasPerEntry {
  def getFeatureName(): String = "mean-commas-per-entry"
}
case class MeanCommasPerEntry() extends SingleFeatureValuesExtractor {
  override def getFeatureName(): String = MeanCommasPerEntry.getFeatureName

  override def computeFeature(attribute: PreprocessedAttribute): Double = {
    val attrContent = attribute.rawAttribute.values.filter { x: String => x.nonEmpty}
    if(attrContent.nonEmpty) {
      val counts = attrContent.map(_.count(_ == ',').toDouble)
      counts.sum / counts.size
    } else {
      -1.0
    }
  }

  override def computeFeatureValues(attr: PreprocessedValues): Double = {
    val attrContent = attr.values.filter(_.nonEmpty)
    if(attrContent.nonEmpty) {
      val counts = attrContent.map(_.count(_ == ',').toDouble)
      counts.sum / counts.length
    } else {
      -1.0
    }
  }
}

object MeanForwardSlashesPerEntry {
  def getFeatureName(): String = "mean-forward-slashes-per-entry"
}
case class MeanForwardSlashesPerEntry() extends SingleFeatureValuesExtractor {
  override def getFeatureName(): String = MeanForwardSlashesPerEntry.getFeatureName

  override def computeFeature(attribute: PreprocessedAttribute): Double = {
    val attrContent = attribute.rawAttribute.values.filter { x: String => x.nonEmpty}
    if(attrContent.nonEmpty) {
      val counts = attrContent.map(_.count(_ == '/').toDouble)
      counts.sum / counts.size
    } else {
      -1.0
    }
  }

  override def computeFeatureValues(attr: PreprocessedValues): Double = {
    val attrContent = attr.values.filter(_.nonEmpty)
    if(attrContent.nonEmpty) {
      val counts = attrContent.map(_.count(_ == '/').toDouble)
      counts.sum / counts.length
    } else {
      -1.0
    }
  }
}

object PropRangeFormat {
  def getFeatureName(): String = "prop-range-format"
}
case class PropRangeFormat() extends SingleFeatureValuesExtractor {
  override def getFeatureName(): String = PropRangeFormat.getFeatureName

  override def computeFeature(attribute: PreprocessedAttribute): Double = {
    val rangeFmt = "([0-9]+)-([0-9]+)".r
    val attrContent = attribute.rawAttribute.values.filter { x: String => x.nonEmpty}
    if(attrContent.nonEmpty) {
      val valsWithRangeFmt = attrContent.filter({
        case rangeFmt(start, end) => start.toDouble <= end.toDouble
        case _ => false
      })
      valsWithRangeFmt.size.toDouble / attrContent.size.toDouble
    } else {
      -1.0
    }
  }

  override def computeFeatureValues(attr: PreprocessedValues): Double = {
    val rangeFmt = "([0-9]+)-([0-9]+)".r
    val attrContent = attr.values.filter(_.nonEmpty)
    if(attrContent.nonEmpty) {
      val valsWithRangeFmt = attrContent.filter {
        case rangeFmt(start, end) => start.toDouble <= end.toDouble
        case _ => false
      }
      valsWithRangeFmt.length.toDouble / attrContent.length.toDouble
    } else {
      -1.0
    }
  }
}


/**
  *  This feature was taken from the paper: "Semantic Integration in Heterogenous
  *  Databases Using Neural Networks" by Wen-Syan and Chris Clifton.
  **/
object NumericalCharRatioFeatureExtractor {
  def getFeatureName = "prop-numerical-chars"
}
case class NumericalCharRatioFeatureExtractor() extends SingleFeatureValuesExtractor {
  override def getFeatureName() = NumericalCharRatioFeatureExtractor.getFeatureName

  override def computeFeature(attribute: PreprocessedAttribute): Double = {
    val numRegex = "[0-9]".r
    val ratios = attribute.rawAttribute.values.map {
      case s if s.length > 0 => numRegex.findAllIn(s).size.toDouble / s.length
      case _ => 0.0
    }
    ratios.sum / ratios.size
  }

  override def computeFeatureValues(attr: PreprocessedValues): Double = {
    val numRegex = "[0-9]".r
    val ratios: Array[Double] = attr.values.map {
      case s if s.nonEmpty => numRegex.findAllIn(s).size.toDouble / s.length
      case _ => 0.0
    }
    ratios.sum / ratios.length
  }
}


object WhitespaceRatioFeatureExtractor {
  def getFeatureName = "prop-whitespace-chars"
}
case class WhitespaceRatioFeatureExtractor() extends SingleFeatureValuesExtractor {
  override def getFeatureName() = WhitespaceRatioFeatureExtractor.getFeatureName

  override def computeFeature(attribute: PreprocessedAttribute): Double = {
    val numRegex = """\s""".r
    val ratios = attribute.rawAttribute.values.map {
      case s if s.length > 0 => numRegex.findAllIn(s).size.toDouble / s.length
      case _ => 0.0
    }
    ratios.sum / ratios.size
  }

  override def computeFeatureValues(attr: PreprocessedValues): Double = {
    val numRegex = """\s""".r
    val ratios = attr.values.map {
      case s if s.nonEmpty => numRegex.findAllIn(s).size.toDouble / s.length
      case _ => 0.0
    }
    ratios.sum / ratios.length
  }
}


object DiscreteTypeFeatureExtractor {
  def getFeatureName(): String = "is-discrete"
}
case class DiscreteTypeFeatureExtractor() extends SingleFeatureExtractor {
  override def getFeatureName() = DiscreteTypeFeatureExtractor.getFeatureName
  override def computeFeature(attribute: PreprocessedAttribute): Double = {
    if(attribute.preprocessedDataMap("is-discrete").asInstanceOf[Boolean]) 1.0 else 0.0
  }
}


/**
  * Information Theoretic features
  */
object EntropyForDiscreteDataFeatureExtractor {
  def getFeatureName(): String = "entropy-for-discrete-values"
}
case class EntropyForDiscreteDataFeatureExtractor() extends SingleFeatureExtractor {
  override def getFeatureName() = EntropyForDiscreteDataFeatureExtractor.getFeatureName
  override def computeFeature(attribute: PreprocessedAttribute): Double = {
    attribute.preprocessedDataMap("entropy").asInstanceOf[Double]
  }
}


object DatePatternFeatureExtractor {
  def getFeatureName(): String = "prop-datepattern"
}
case class DatePatternFeatureExtractor() extends SingleFeatureValuesExtractor {
  val maxSampleSize = 100

  val datePattern1 = """^[0-9]+/[0-9]+/[0-9]+$""".r
  val datePattern2 = """^[a-zA-Z]+ [0-9]+, [0-9]+$""".r
  val datePattern3 = """[0-9]+:[0-9]+:[0-9]+$""".r
  val datePattern4 = """[0-9]+:[0-9]+$""".r

  override def getFeatureName() = DatePatternFeatureExtractor.getFeatureName

  override def computeFeature(attribute: PreprocessedAttribute): Double = {
    val rawAttr = attribute.rawAttribute
    if(rawAttr.values.isEmpty) {
      0.0
    } else {
      val numSample = if(rawAttr.values.size > maxSampleSize) maxSampleSize else rawAttr.values.size
      val randIdx = (new Random(124213)).shuffle(rawAttr.values.indices.toList).take(numSample)
      val regexResults = randIdx.filter {
        idx => datePattern1.pattern.matcher(rawAttr.values(idx)).matches ||
          datePattern2.pattern.matcher(rawAttr.values(idx)).matches ||
          datePattern3.pattern.matcher(rawAttr.values(idx)).matches ||
          datePattern4.pattern.matcher(rawAttr.values(idx)).matches
      }
      regexResults.size.toDouble / randIdx.size
    }
  }

  override def computeFeatureValues(attr: PreprocessedValues): Double = {
    if(attr.values.isEmpty) {
      0.0
    } else {
      val numSample = if(attr.values.length > maxSampleSize) maxSampleSize else attr.values.length
      val randIdx = (new Random(124213)).shuffle(attr.values.indices.toList).take(numSample)
      val regexResults = randIdx.filter {
        idx => datePattern1.pattern.matcher(attr.values(idx)).matches ||
          datePattern2.pattern.matcher(attr.values(idx)).matches ||
          datePattern3.pattern.matcher(attr.values(idx)).matches ||
          datePattern4.pattern.matcher(attr.values(idx)).matches
      }
      regexResults.size.toDouble / randIdx.size
    }
  }
}


object CharDistFeatureExtractor {
  def chars = "abcdefghijklmnopqrstuvwxyz0123456789~!@#$%^&*()_+-=[]\\{}|;':\",./<>?"
  def getGroupName() = "char-dist-features"
  def getFeatureNames() = (1 to chars.length).map({"char-dist-" + _}).toList
}
case class CharDistFeatureExtractor() extends GroupFeatureLimExtractor {

  override def getGroupName(): String =
    CharDistFeatureExtractor.getGroupName

  override def getFeatureNames(): List[String] =
    CharDistFeatureExtractor.getFeatureNames

  override def computeFeaturesLim(attribute: LimPreprocessedAttribute): List[Double] = {
    val normalisedCharDists = attribute.charDist
    if (normalisedCharDists.isEmpty) {
      CharDistFeatureExtractor.chars.map { _ => 0.0}.toList
    } else {
      CharDistFeatureExtractor.chars.toList.map {
        c => normalisedCharDists.getOrElse(c, 0.0)
      }
    }
  }

  override def computeFeatures(attribute: PreprocessedAttribute): List[Double] = {
    val attrContent: Seq[String] = attribute.rawAttribute.values.filter(_.nonEmpty)
    if(attrContent.nonEmpty) {
      val normalisedCharDists = attribute
        .preprocessedDataMap.getOrElse("normalised-char-frequency-vector", Map())
        .asInstanceOf[Map[Char,Double]]

      // this calculation leads to the fact that only chars present in the last row are in the distribution
//      val summedCharDists = attrContent.foldLeft[Map[Char,Double]](Map.empty[Char,Double])(
//        (props: Map[Char,Double], nextVal: String) => {
//          val charDist = computeNormalizedCharFreq(nextVal)
//          val newMap: Map[Char,Double] = charDist.keys.map {
//            k: Char =>
//              k -> (props.getOrElse(k,0.0) + charDist(k))
//          }.toMap
//          newMap
//        }
//      )
//      val normalisedCharDists = normalise(summedCharDists)

      CharDistFeatureExtractor.chars.toList.map {
        c => normalisedCharDists.getOrElse(c, 0.0)
      }
    } else {
      CharDistFeatureExtractor.chars.map { _ => 0.0}.toList
    }
  }

  def computeNormalizedCharFreq(s: String): Map[Char,Double] = {
//    val counts = scala.collection.mutable.Map[Char, Double]()
//    s.toCharArray.foreach({case c =>
//      counts += (c -> (counts.getOrElse(c, 0.0) + 1.0))
//    })
    // calculate frequencies of chars in the attribute values (rows of the column)
    val counts: Map[Char,Double] = s
      .toCharArray
      .groupBy(_.toChar)
      .mapValues(_.length.toDouble)

    if(counts.nonEmpty) {
      normalise(counts)
    } else {
      Map()
    }
  }

  def normalise(counts: Map[Char,Double]): Map[Char,Double] = {
    val maxCount = counts.values.max
    val dscaled = counts.values
      .map(_ /maxCount) // normalize counts
      .foldLeft(0.0)(_ + Math.pow(_, 2)) // compute sum of squares

    val norm = Math.sqrt(dscaled) * maxCount // the norm of the counts
    val normCf = norm match {
      case 0 => counts.map { case (x, _) => (x,0.0) }
      case normbig =>
        counts.map { case (x, y) => (x, y.toDouble / normbig) }
    }

//    val length = Math.abs(Math.sqrt(normCf.values.map({x=>x*x}).sum) - 1.0)
//    assert((length - 1.0) <= 0.00005,
//      "length of char freq vector is " + length
//        + "\nnorm: " + norm
//        + "\ncounts: " + counts
//        + "\ncounts^2: " + (counts.values.map({case x => x*x})))
    normCf
  }
}


/**
  *  Data type indicator fields
  **/
object DataTypeFeatureExtractor {
  def getGroupName(): String = "inferred-data-type"
}
case class DataTypeFeatureExtractor(typeMapFile: Option[String] = None
                                   ) extends GroupFeatureLimExtractor with LazyLogging {
  val keys = List(
    ("inferred-type-float", "float"),
    ("inferred-type-integer", "integer"),
    ("inferred-type-long", "long"),
    ("inferred-type-boolean", "boolean"),
    ("inferred-type-date", "date"),
    ("inferred-type-time", "time"),
    ("inferred-type-datetime", "datetime"),
    ("inferred-type-string", "string")
  )

  val typeMap: Option[Map[String,String]] =
    typeMapFile.map { f =>
      val lines = Source.fromFile(f).getLines.toList.drop(1)
      lines.map {
        l =>
          val toks = l.split(",")
          (toks(0), toks(1))
      }.toMap
    }

  //print out type map
  logger.info(typeMap.map({
    "***Type Map Supplied:\n\t" + _.mkString("\n\t")
  }).getOrElse("***No Type Map Supplied."))


  override def getGroupName(): String = DataTypeFeatureExtractor.getGroupName

  override def getFeatureNames(): List[String] = keys.map(_._1)

  override def computeFeaturesLim(attribute: LimPreprocessedAttribute): List[Double] = {
    typeMap.flatMap {
      m =>
        val predefinedType = m.get(attribute.attributeName)
        predefinedType.map {
          t =>
            keys.map {
              case (fname, typename) => if(typename.equalsIgnoreCase(t)) 1.0 else 0.0
            }
        }
    }.getOrElse(
      keys.map {
        case (featureName, typeName) =>
          if(attribute.inferredMap(featureName)) 1.0 else 0.0
      })
  }

  override def computeFeatures(attribute: PreprocessedAttribute): List[Double] = {
    typeMap.flatMap {
      m =>
      val predefinedType = m.get(attribute.rawAttribute.id)
      predefinedType.map {
        t =>
          keys.map {
            case (fname, typename) => if(typename.equalsIgnoreCase(t)) 1.0 else 0.0
          }
      }
    }.getOrElse(
      keys.map {
        case (featureName, typeName) =>
          if(attribute.preprocessedDataMap(featureName).asInstanceOf[Boolean]) 1.0 else 0.0
      }
    )
  }
}


/**
  *  String Length Statistics.
  **/
object TextStatsFeatureExtractor {
  def getGroupName(): String = "stats-of-text-length"
}
case class TextStatsFeatureExtractor() extends GroupFeatureExtractor {

  override def getGroupName(): String = TextStatsFeatureExtractor.getGroupName

  override def getFeatureNames(): List[String] =
    List("stringLengthMean","stringLengthMedian",
      "stringLengthMode","stringLengthMin","stringLengthMax")

  override def computeFeatures(attribute: PreprocessedAttribute): List[Double] = {
    attribute.preprocessedDataMap("string-length-stats").asInstanceOf[List[Double]]
  }
}

/**
  *  Numerical values statistics.
  **/
object NumberTypeStatsFeatureExtractor {
  def getGroupName(): String = "stats-of-numerical-type"
}
case class NumberTypeStatsFeatureExtractor() extends GroupFeatureFullExtractor {
  val floatRegex = """(^[+-]?[0-9]*\.[0-9]+)|(^[+-]?[0-9]+)""".r

  override def getGroupName(): String =
    NumberTypeStatsFeatureExtractor.getGroupName

  override def getFeatureNames(): List[String] =
    List("numTypeMean","numTypeMedian",
      "numTypeMode","numTypeMin","numTypeMax")

  override def computeFeatureFull(attribute: FullPreprocessedAttribute): List[Double] = {
    if(attribute.inferredMap("inferred-type-integer") ||
      attribute.inferredMap("inferred-type-float") ||
      attribute.inferredMap("inferred-type-long")) {
      val values = attribute.values
        .filter{
          x => x.nonEmpty && floatRegex.pattern.matcher(x).matches
        }
        .map(_.toDouble)
      val mean = values.sum / values.size.toDouble

      val sortedValues = values.sorted
      val median = sortedValues(Math.ceil(values.size.toDouble/2.0).toInt - 1)

      val mode = values.groupBy(identity).maxBy(_._2.size)._1

      val max = values.foldLeft(0.0)({case (mx,v) => {if(v > mx) v else mx}})
      val min = values.foldLeft(max)({case (mn,v) => {if(v < mn) v else mn}})
      List(mean,median,mode,min,max)
    } else {
      List(-1.0,-1.0,-1.0,-1.0,-1.0)
    }
  }

  override def computeFeatures(attribute: PreprocessedAttribute): List[Double] = {
    if(attribute.preprocessedDataMap("inferred-type-integer").asInstanceOf[Boolean] ||
      attribute.preprocessedDataMap("inferred-type-float").asInstanceOf[Boolean] ||
      attribute.preprocessedDataMap("inferred-type-long").asInstanceOf[Boolean]) {
      val values = attribute.rawAttribute.values
        .filter{
          x => x.nonEmpty && floatRegex.pattern.matcher(x).matches
        }
        .map(_.toDouble)
      val mean = values.sum / values.size.toDouble

      val sortedValues = values.sorted
      val median = sortedValues(Math.ceil(values.size.toDouble/2.0).toInt - 1)

      val mode = values.groupBy(identity).maxBy(_._2.size)._1

      val max = values.foldLeft(0.0)({case (mx,v) => {if(v > mx) v else mx}})
      val min = values.foldLeft(max)({case (mn,v) => {if(v < mn) v else mn}})
      List(mean,median,mode,min,max)
    } else {
      List(-1.0,-1.0,-1.0,-1.0,-1.0)
    }
  }
}
