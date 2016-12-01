package com.nicta.dataint.matcher.features

import com.nicta.dataint.data._
import com.nicta.dataint.matcher.train.TrainAliases._
import com.nicta.dataint.matcher.train.TrainingSettings
import com.nicta.dataint.matcher._

import com.typesafe.scalalogging.LazyLogging
import scala.util._
import scala.io._

import edu.cmu.lti.ws4j._

trait FeatureExtractor

object FeatureExtractorUtil extends LazyLogging {
    def getFeatureNames(features: List[FeatureExtractor]
                       ): List[String] = {
        features.flatMap({
            case x: SingleFeatureExtractor => List(x.getFeatureName())
            case x: GroupFeatureExtractor => x.getFeatureNames()
        }).toList
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
                       )(implicit d: DummyImplicit): List[(PreprocessedAttribute, List[Any], String)] = {
        logger.info(s"Extracting features from ${preprocessedAttributes.size} instances...")
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


    def generateFeatureExtractors(classes: List[String],
                                  preprocessedAttributes: List[PreprocessedAttribute],
                                  trainingSettings: TrainingSettings,
                                  labels: SemanticTypeLabels
                                 ) = {
         createStandardFeatureExtractors(trainingSettings.featureSettings) ++ createExampleBasedFeatureExtractors(preprocessedAttributes, labels, classes, trainingSettings.featureSettings)        
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
        val attrsWithNames = trainingData.filter({_.rawAttribute.metadata.map({x => true}).getOrElse(false)})

        val factoryMethods = List(
            (RfKnnFeatureExtractor.getGroupName, {
                lazy val auxData = attrsWithNames.map({
                    case attribute => RfKnnFeature(attribute.rawAttribute.id,
                                                   attribute.rawAttribute.metadata.get.name,
                                                   labels.findLabel(attribute.rawAttribute.id))})
                () => {
                    val k = featureSettings.featureExtractorParams(RfKnnFeatureExtractor.getGroupName)("num-neighbours").toInt
                    RfKnnFeatureExtractor(classes, auxData, k)
                }
            }),
            (MinEditDistFromClassExamplesFeatureExtractor.getGroupName, {
                lazy val auxData = attrsWithNames.map({
                    case attribute => (labels.findLabel(attribute.rawAttribute.id), attribute.rawAttribute.metadata.get.name)
                }).groupBy({_._1}).map({case (className,values) => (className, values.map({_._2}))}).toMap
                () => MinEditDistFromClassExamplesFeatureExtractor(classes, auxData)
            }),
            (MeanCharacterCosineSimilarityFeatureExtractor.getGroupName, {
                lazy val auxData = attrsWithNames.map({
                    case attribute => (labels.findLabel(attribute.rawAttribute.id), computeCharDistribution(attribute.rawAttribute))
                }).groupBy({_._1}).map({case (className,values) => (className, values.map({_._2}))}).toMap
                () => MeanCharacterCosineSimilarityFeatureExtractor(classes, auxData)
            }),
            (JCNMinWordNetDistFromClassExamplesFeatureExtractor.getGroupName, {
                lazy val auxData = attrsWithNames.map({case attr =>
                    val attrId = attr.rawAttribute.id
                    val label = labels.findLabel(attrId)
                    (label, (attr.preprocessedDataMap("attribute-name-tokenized").asInstanceOf[List[String]]))
                }).groupBy({_._1}).map({case (className, values) => (className, values.map({_._2}))}).toMap
                () => {
                    val maxComparisons = featureSettings.featureExtractorParams(JCNMinWordNetDistFromClassExamplesFeatureExtractor.getGroupName)("max-comparisons-per-class").toInt
                    JCNMinWordNetDistFromClassExamplesFeatureExtractor(classes, auxData, maxComparisons)
                }
            }),
            (LINMinWordNetDistFromClassExamplesFeatureExtractor.getGroupName, {
                lazy val auxData = attrsWithNames.map({case attr =>
                    val attrId = attr.rawAttribute.id
                    val label = labels.findLabel(attrId)
                    (label, (attr.preprocessedDataMap("attribute-name-tokenized").asInstanceOf[List[String]]))
                }).groupBy({_._1}).map({case (className, values) => (className, values.map({_._2}))}).toMap
                () => {
                    val maxComparisons = featureSettings.featureExtractorParams(LINMinWordNetDistFromClassExamplesFeatureExtractor.getGroupName)("max-comparisons-per-class").toInt
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
        val counts = scala.collection.mutable.Map[Char, Int]()
        attribute.values.foreach({case v =>
            v.toCharArray.foreach({case c =>
                counts += (c -> (counts.getOrElse(c, 0) + 1))
            })
        })

        if(counts.size > 0) {
            //we downscale the counts vector by 1/maxCount, compute the norm, then upscale by maxCount to prevent overflow
            val maxCount = counts.values.max
            val dscaledCounts = counts.map({case (k,v) => (k,v.toDouble/maxCount.toDouble)})
            val norm = Math.sqrt((dscaledCounts.values.map({case x => x*x}).sum)) * maxCount
            val normCf = counts.keys.map({case x => (x, if(norm != 0) counts(x).toDouble/norm else 0)}).toMap

            val length = Math.abs(Math.sqrt(normCf.values.map({x=>x*x}).sum) - 1.0)
            assert(Math.abs(Math.sqrt(normCf.values.map({x=>x*x}).sum) - 1.0) <= 0.00005,
                "length of char freq vector is " + length
                + "\nnorm: " + norm
                + "\ncounts: " + counts
                + "\ncounts^2: " + (counts.values.map({case x => x*x})))
            normCf
        } else {
            Map()
        }
    }

    def printFeatureExtractorNames(featureExtractors: List[FeatureExtractor]
                                  ) = {
        logger.info("Features Used:\n\t")
        logger.info(featureExtractors.map({
            case fe: SingleFeatureExtractor => fe.getFeatureName
            case gfe: GroupFeatureExtractor => gfe.getGroupName
        }).mkString("\n\t"))
    }
}

trait SingleFeatureExtractor extends FeatureExtractor {
    def getFeatureName(): String
    def computeFeature(attribute: PreprocessedAttribute): Double
}

trait GroupFeatureExtractor extends FeatureExtractor {
    def getGroupName(): String
    def getFeatureNames(): List[String]
    def computeFeatures(attribute: PreprocessedAttribute): List[Double]
}


object NumUniqueValuesFeatureExtractor {
    def getFeatureName() = "num-unique-vals"
}
case class NumUniqueValuesFeatureExtractor() extends SingleFeatureExtractor {
    override def getFeatureName() = NumUniqueValuesFeatureExtractor.getFeatureName

    override def computeFeature(attribute: PreprocessedAttribute): Double = {
        attribute.rawAttribute.values.map({_.toLowerCase.trim}).distinct.size
    }
}


object PropUniqueValuesFeatureExtractor {
    def getFeatureName() = "prop-unique-vals"
}
case class PropUniqueValuesFeatureExtractor() extends SingleFeatureExtractor {
    override def getFeatureName() = PropUniqueValuesFeatureExtractor.getFeatureName

    override def computeFeature(attribute: PreprocessedAttribute): Double = {
        val values = attribute.rawAttribute.values
        values.map({_.toLowerCase.trim}).distinct.size.toDouble / values.size.toDouble
    }
}


object PropMissingValuesFeatureExtractor {
    def getFeatureName() = "prop-missing-vals"
}
case class PropMissingValuesFeatureExtractor() extends SingleFeatureExtractor {
    override def getFeatureName() = PropMissingValuesFeatureExtractor.getFeatureName

    override def computeFeature(attribute: PreprocessedAttribute): Double = {
        val values = attribute.rawAttribute.values
        values.filter({_.trim.length == 0}).size.toDouble / values.size.toDouble
    }
}


object PropAlphaCharsFeatureExtractor {
    def getFeatureName() = "ratio-alpha-chars"
}
case class PropAlphaCharsFeatureExtractor() extends SingleFeatureExtractor {
    override def getFeatureName() = PropAlphaCharsFeatureExtractor.getFeatureName

    override def computeFeature(attribute: PreprocessedAttribute): Double = {
        val attrContent = attribute.rawAttribute.values.mkString("")
        if(attrContent.length > 0) {
            attrContent.replaceAll("[^a-zA-Z]","").length.toDouble / attrContent.length.toDouble
        } else {
            -1.0
        }
    }
}


object PropEntriesWithAtSign {
    def getFeatureName() = "prop-entries-with-at-sign"
}
case class PropEntriesWithAtSign() extends SingleFeatureExtractor {
    override def getFeatureName() = PropEntriesWithAtSign.getFeatureName

    override def computeFeature(attribute: PreprocessedAttribute): Double = {
        val attrContent = attribute.rawAttribute.values.filter({case x: String => x.length > 0})
        if(attrContent.size > 0) {
            attrContent.filter({
                case x: String if(x.contains("@")) => true
                case _ => false
            }).size.toDouble / attrContent.size.toDouble
        } else {
            -1.0
        }
        
    }
}


object PropEntriesWithCurrencySymbol {
    def getFeatureName() = "prop-entries-with-currency-symbol"
}
case class PropEntriesWithCurrencySymbol() extends SingleFeatureExtractor {
    val currencySymbols = List("$","AUD")

    override def getFeatureName() = PropEntriesWithCurrencySymbol.getFeatureName

    override def computeFeature(attribute: PreprocessedAttribute): Double = {
        val attrContent = attribute.rawAttribute.values.filter({case x: String => x.length > 0})
        if(attrContent.size > 0) {
            attrContent.filter({
                case x: String if(currencySymbols.map({x.contains(_)}).foldLeft(false)({_ || _})) => true
                case _ => false
            }).size.toDouble / attrContent.size.toDouble
        } else {
            -1.0
        }
        
    }
}


object PropEntriesWithHyphen {
    def getFeatureName() = "prop-entries-with-hyphen"
}
case class PropEntriesWithHyphen() extends SingleFeatureExtractor {
    override def getFeatureName() = PropEntriesWithHyphen.getFeatureName

    override def computeFeature(attribute: PreprocessedAttribute): Double = {
        val attrContent = attribute.rawAttribute.values.filter({case x: String => x.length > 0})
        if(attrContent.size > 0) {
            attrContent.filter({
                case x: String if(x.contains("-")) => true
                case _ => false
            }).size.toDouble / attrContent.size.toDouble
        } else {
            -1.0
        }
        
    }
}

object PropEntriesWithParen {
    def getFeatureName() = "prop-entries-with-paren"
}
case class PropEntriesWithParen() extends SingleFeatureExtractor {
    override def getFeatureName() = PropEntriesWithParen.getFeatureName

    override def computeFeature(attribute: PreprocessedAttribute): Double = {
        val attrContent = attribute.rawAttribute.values.filter({case x: String => x.length > 0})
        if(attrContent.size > 0) {
            attrContent.filter({
                case x: String if(x.contains("(") || x.contains(")")) => true
                case _ => false
            }).size.toDouble / attrContent.size.toDouble
        } else {
            -1.0
        }
        
    }
}

object MeanCommasPerEntry {
    def getFeatureName() = "mean-commas-per-entry"
}
case class MeanCommasPerEntry() extends SingleFeatureExtractor {
    override def getFeatureName() = MeanCommasPerEntry.getFeatureName

    override def computeFeature(attribute: PreprocessedAttribute): Double = {
        val attrContent = attribute.rawAttribute.values.filter({case x: String => x.length > 0})
        if(attrContent.size > 0) {
            val counts = attrContent.map({_.count(_ == ',').toDouble})
            counts.sum / counts.size.toDouble
        } else {
            -1.0
        }
        
    }
}

object MeanForwardSlashesPerEntry {
    def getFeatureName() = "mean-forward-slashes-per-entry"
}
case class MeanForwardSlashesPerEntry() extends SingleFeatureExtractor {
    override def getFeatureName() = MeanForwardSlashesPerEntry.getFeatureName

    override def computeFeature(attribute: PreprocessedAttribute): Double = {
        val attrContent = attribute.rawAttribute.values.filter({case x: String => x.length > 0})
        if(attrContent.size > 0) {
            val counts = attrContent.map({_.count(_ == '/').toDouble})
            counts.sum / counts.size.toDouble
        } else {
            -1.0
        }
        
    }
}

object PropRangeFormat {
    def getFeatureName() = "prop-range-format"
}
case class PropRangeFormat() extends SingleFeatureExtractor {
    override def getFeatureName() = PropRangeFormat.getFeatureName

    override def computeFeature(attribute: PreprocessedAttribute): Double = {
        val rangeFmt = "([0-9]+)-([0-9]+)".r

        val attrContent = attribute.rawAttribute.values.filter({case x: String => x.length > 0})

        if(attrContent.size > 0) {
            val valsWithRangeFmt = attrContent.filter({
                case rangeFmt(start, end) => start.toDouble <= end.toDouble
                case _ => false
            })
            valsWithRangeFmt.size.toDouble / attrContent.size.toDouble
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
case class NumericalCharRatioFeatureExtractor() extends SingleFeatureExtractor {
    override def getFeatureName() = NumericalCharRatioFeatureExtractor.getFeatureName

    override def computeFeature(attribute: PreprocessedAttribute): Double = {
        val numRegex = "[0-9]".r
        val ratios = attribute.rawAttribute.values.map({
            case s if s.length > 0 => numRegex.findAllIn(s).size.toDouble / s.size.toDouble
            case _ => 0
        })
        ratios.sum.toDouble / ratios.size.toDouble
    }
}


object WhitespaceRatioFeatureExtractor {
    def getFeatureName = "prop-whitespace-chars"
}
case class WhitespaceRatioFeatureExtractor() extends SingleFeatureExtractor {
    override def getFeatureName() = WhitespaceRatioFeatureExtractor.getFeatureName

    override def computeFeature(attribute: PreprocessedAttribute): Double = {
        val numRegex = """\s""".r
        val ratios = attribute.rawAttribute.values.map({
            case s if s.length > 0 => numRegex.findAllIn(s).size.toDouble / s.size.toDouble
            case _ => 0
        })
        ratios.sum.toDouble / ratios.size
    }
}


object DiscreteTypeFeatureExtractor {
    def getFeatureName() = "is-discrete"
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
    def getFeatureName() = "entropy-for-discrete-values"
}
case class EntropyForDiscreteDataFeatureExtractor() extends SingleFeatureExtractor {
    override def getFeatureName() = EntropyForDiscreteDataFeatureExtractor.getFeatureName
    override def computeFeature(attribute: PreprocessedAttribute): Double = {
        attribute.preprocessedDataMap("entropy").asInstanceOf[Double]
    }
}


object DatePatternFeatureExtractor {
    def getFeatureName() = "prop-datepattern"
}
case class DatePatternFeatureExtractor() extends SingleFeatureExtractor {
    val maxSampleSize = 100

    val datePattern1 = """^[0-9]+/[0-9]+/[0-9]+$""".r
    val datePattern2 = """^[a-zA-Z]+ [0-9]+, [0-9]+$""".r
    val datePattern3 = """[0-9]+:[0-9]+:[0-9]+$""".r
    val datePattern4 = """[0-9]+:[0-9]+$""".r

    override def getFeatureName() = DatePatternFeatureExtractor.getFeatureName

    override def computeFeature(attribute: PreprocessedAttribute): Double = {
        val rawAttr = attribute.rawAttribute
        if(rawAttr.values.size == 0) {
            0.0
        } else {
            val numSample = if(rawAttr.values.size > maxSampleSize) maxSampleSize else rawAttr.values.size
            val randIdx = (new Random(124213)).shuffle((0 until rawAttr.values.size).toList).take(numSample)
            val regexResults = randIdx.filter({
                case idx if (datePattern1.pattern.matcher(rawAttr.values(idx)).matches || 
                    datePattern2.pattern.matcher(rawAttr.values(idx)).matches || 
                    datePattern3.pattern.matcher(rawAttr.values(idx)).matches || 
                    datePattern4.pattern.matcher(rawAttr.values(idx)).matches
                ) => true
                case _ => false
            })
            regexResults.size.toDouble / randIdx.size.toDouble
        }
    }
}


object CharDistFeatureExtractor {
    def chars = "abcdefghijklmnopqrstuvwxyz0123456789~!@#$%^&*()_+-=[]\\{}|;':\",./<>?"
    def getGroupName() = "char-dist-features"
    def getFeatureNames() = (1 to chars.length).map({"char-dist-" + _}).toList
}
case class CharDistFeatureExtractor() extends GroupFeatureExtractor {
    override def getGroupName() = CharDistFeatureExtractor.getGroupName
    override def getFeatureNames() = CharDistFeatureExtractor.getFeatureNames

    override def computeFeatures(attribute: PreprocessedAttribute): List[Double] = {
        val attrContent: Seq[String] = attribute.rawAttribute.values.filter({case x: String => x.length > 0})
        if(attrContent.size > 0) {
            val summedCharDists = attrContent.foldLeft[Map[Char,Double]](Map.empty[Char,Double])(
                (props: Map[Char,Double], nextVal: String) => {
                    val charDist = computeNormalizedCharFreq(nextVal)
                    val newMap: Map[Char,Double] = charDist.keys.map({case k: Char =>
                        (k -> (props.getOrElse(k,0.0) + charDist(k)))
                    }).toMap
                    newMap
                }
            )
            val normalisedCharDists = normalise(summedCharDists)
            CharDistFeatureExtractor.chars.toList.map({case c => normalisedCharDists.getOrElse(c, 0.0)}).toList
        } else {
            CharDistFeatureExtractor.chars.map({case x => 0.0}).toList
        }
    }

    def computeNormalizedCharFreq(s: String): Map[Char,Double] = {
        val counts = scala.collection.mutable.Map[Char, Double]()
        s.toCharArray.foreach({case c =>
            counts += (c -> (counts.getOrElse(c, 0.0) + 1.0))
        })

        if(counts.size > 0) {
            normalise(counts.toMap)
        } else {
            Map()
        }
    }

    def normalise(counts: Map[Char,Double]): Map[Char,Double] = {
        val maxCount = counts.values.max
        val dscaledCounts = counts.map({case (k,v) => (k,v.toDouble/maxCount.toDouble)})
        val norm = Math.sqrt((dscaledCounts.values.map({case x => x*x}).sum)) * maxCount
        val normCf = counts.keys.map({case x => (x, if(norm != 0) counts(x).toDouble/norm else 0)}).toMap

        val length = Math.abs(Math.sqrt(normCf.values.map({x=>x*x}).sum) - 1.0)
        assert((length - 1.0) <= 0.00005, 
            "length of char freq vector is " + length 
            + "\nnorm: " + norm
            + "\ncounts: " + counts 
            + "\ncounts^2: " + (counts.values.map({case x => x*x})))
        normCf
    }
}


/**
 *  Data type indicator fields
 **/
object DataTypeFeatureExtractor {
    def getGroupName() = "inferred-data-type"
}
case class DataTypeFeatureExtractor(typeMapFile: Option[String] = None) extends GroupFeatureExtractor with LazyLogging {
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

    val typeMap: Option[Map[String,String]] = typeMapFile.map({f => 
        val lines = Source.fromFile(f).getLines.toList.drop(1)
        lines.map({case l => 
            val toks = l.split(",")
            (toks(0), toks(1))
        }).toMap
    })

    //print out type map
    logger.info(typeMap.map({
        "***Type Map Supplied:\n\t" + _.mkString("\n\t")
    }).getOrElse("***No Type Map Supplied."))
    

    override def getGroupName() = DataTypeFeatureExtractor.getGroupName
    override def getFeatureNames() = keys.map({_._1})
    override def computeFeatures(attribute: PreprocessedAttribute): List[Double] = {
        typeMap.flatMap({m => 
            val predefinedType = m.get(attribute.rawAttribute.id)
            predefinedType.map({case t =>
                keys.map({case (fname, typename) => if(typename.equalsIgnoreCase(t)) 1.0 else 0.0})
            })
        }).getOrElse({
            keys.map({case (featureName, typeName) => 
                if(attribute.preprocessedDataMap(featureName).asInstanceOf[Boolean]) 1.0 else 0.0
            })
        }).toList
    }
}


/**
 *  String Length Statistics.
 **/
 object TextStatsFeatureExtractor {
    def getGroupName() = "stats-of-text-length"
 }
case class TextStatsFeatureExtractor() extends GroupFeatureExtractor {
    override def getGroupName() = TextStatsFeatureExtractor.getGroupName
    override def getFeatureNames() = List("stringLengthMean","stringLengthMedian",
                                                 "stringLengthMode","stringLengthMin","stringLengthMax")

    override def computeFeatures(attribute: PreprocessedAttribute): List[Double] = {
        attribute.preprocessedDataMap("string-length-stats").asInstanceOf[List[Double]]
    }    
}

/**
 *  Numerical values statistics.
 **/
 object NumberTypeStatsFeatureExtractor {
    def getGroupName() = "stats-of-numerical-type"
 }
case class NumberTypeStatsFeatureExtractor() extends GroupFeatureExtractor {
    val floatRegex = """(^[+-]?[0-9]*\.[0-9]+)|(^[+-]?[0-9]+)""".r

    override def getGroupName() = NumberTypeStatsFeatureExtractor.getGroupName
    override def getFeatureNames() = List("numTypeMean","numTypeMedian",
                                                 "numTypeMode","numTypeMin","numTypeMax")

    override def computeFeatures(attribute: PreprocessedAttribute): List[Double] = {
        if(attribute.preprocessedDataMap("inferred-type-integer").asInstanceOf[Boolean] ||
            attribute.preprocessedDataMap("inferred-type-float").asInstanceOf[Boolean] ||
            attribute.preprocessedDataMap("inferred-type-long").asInstanceOf[Boolean]) {
            val values = attribute.rawAttribute.values.filter({case x => (x.length > 0) && (floatRegex.pattern.matcher(x).matches)}).map({_.toDouble})
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

