package au.csiro.data61.matcher.matcher.runner

import au.csiro.data61.matcher.data._
import au.csiro.data61.matcher.matcher._
import au.csiro.data61.matcher.matcher.features._
import au.csiro.data61.matcher.matcher.eval.datasetutils._

import collection.parallel.ForkJoinTasks.defaultForkJoinPool._

import scala.util.Random
import java.io._

object RunFeatureExtractor {
    val usageMessage = """Usage:
                         #    java RunFeatureExtractor <trainProp> <outputDir>
                       """.stripMargin('#')

    def main(args: Array[String]) = {
        if(args.size < 2) {
            println(usageMessage)
        } else {
            //create output folder if it doesn't exist
            val trainProp = args(0).toDouble
            val outputDir = args(1)
            val dirFile = new File(outputDir)
            if(!dirFile.exists) {
                dirFile.mkdir()
            }

            abstract class NamedDataLoader(val domainName: String) {
                def loadDataSets(): List[DataModel]
                def loadLabels(): Labels
            }

            //specify datasets we want
            val allDataSetLoaders = List(
                (new NamedDataLoader("wisc-realestate") with WISCRealEstateDomain1Utils)
                // (new NamedDataLoader("citycouncil-data") with OpenGovCityCouncilUtils)
            )

            //load all datasets and labels
            println("Loading datasets...")
            val allDataSets = allDataSetLoaders.map({loader => (
                loader.domainName, 
                loader.loadDataSets(), 
                loader.loadSemanticTypeLabels()
            )})
            println("Datasets loaded.")

            //get all attributes
            val allAttributes = allDataSets.flatMap({case (name, database, labels) =>
                database.par.flatMap({datatable => 
                    DataPreprocessor().preprocess(DataModel.getAllAttributes(datatable))
                })
            }).toList

            //split into train/test sets
            val shuffledAttrs = Random.shuffle(allAttributes)
            val dataSize = shuffledAttrs.size
            val trainSize = Math.floor(dataSize * trainProp).toInt
            val testSize = dataSize - trainSize - 1
            val trainSet = shuffledAttrs.take(trainSize)
            val testSet = shuffledAttrs.takeRight(testSize)

            val allLabels = allDataSets.map({_._3}).foldLeft(SemanticTypeLabels(Map()))({
                (allLbls, dsLbls) => (allLbls ++ dsLbls.asInstanceOf[SemanticTypeLabels]).asInstanceOf[SemanticTypeLabels]
            })
            val trainSetAttrIds = trainSet.map({_.rawAttribute.id})
            val (trainLabelsMap, testLabelsMap) = allLabels.labelsMap.partition({case (k,v) => trainSetAttrIds.contains(k)})
            val trainLabels = SemanticTypeLabels(trainLabelsMap)
            val testLabels = SemanticTypeLabels(testLabelsMap)

            val classes = allLabels.labelsMap.values.map({_.getGtClass}).toList.distinct
            val featureExtractors = createFeatureExtractors(trainSet, trainLabels, classes)

            //write headers
            val header = ("id,label" :: featureExtractors.flatMap({
                case fe: SingleFeatureExtractor => List(fe.getFeatureName)
                case gfe: GroupFeatureExtractor => gfe.getFeatureNames
            })).mkString(",")

            //extract features and write to file
            val trainFeatures = FeatureExtractionProcess().extractFeatures(featureExtractors, trainSet)
            val testFeatures = FeatureExtractionProcess().extractFeatures(featureExtractors, testSet)
            save(trainSet, trainFeatures, header, trainLabels, s"$outputDir/train.csv")
            save(testSet, testFeatures, header, testLabels, s"$outputDir/test.csv")

            println("DONE!")
        }
    }

    def createFeatureExtractors(trainingData: List[PreprocessedAttribute], labels: SemanticTypeLabels, classes: List[String]): List[FeatureExtractor] = {
        //specify all fetures we want
        val featureExtractors1 = List(
            NumUniqueValuesFeatureExtractor(),
            PropUniqueValuesFeatureExtractor(),
            PropMissingValuesFeatureExtractor(),
            DataTypeFeatureExtractor(),
            NumericalCharRatioFeatureExtractor(),
            WhitespaceRatioFeatureExtractor(),
            DiscreteTypeFeatureExtractor(),
            EntropyForDiscreteDataFeatureExtractor(),
            PropAlphaCharsFeatureExtractor(),
            PropEntriesWithAtSign(),
            PropEntriesWithCurrencySymbol(),
            PropEntriesWithHyphen(),
            PropEntriesWithParen(),
            MeanCommasPerEntry(),
            MeanForwardSlashesPerEntry(),
            PropRangeFormat(),
            DatePatternFeatureExtractor(),
            TextStatsFeatureExtractor(),
            NumberTypeStatsFeatureExtractor(),
            CharDistFeatureExtractor()
        )

        val attrsWithNames = trainingData.filter({_.rawAttribute.metadata.map({x => true}).getOrElse(false)})
        val featureExtractors2 = List({
                val auxData = attrsWithNames.map({case attribute => 
                    RfKnnFeature(attribute.rawAttribute.id, attribute.rawAttribute.metadata.get.name, labels.findLabel(attribute.rawAttribute.id))
                })
                RfKnnFeatureExtractor(classes, auxData, 3)
            }, {
                val auxData = attrsWithNames.map({
                    case attribute => (labels.findLabel(attribute.rawAttribute.id), attribute.rawAttribute.metadata.get.name)
                }).groupBy({_._1}).map({case (className,values) => (className, values.map({_._2}))}).toMap
                MinEditDistFromClassExamplesFeatureExtractor(classes, auxData)
            }, {
                val auxData = attrsWithNames.map({
                    case attribute => (labels.findLabel(attribute.rawAttribute.id), FeatureExtractorUtil.computeCharDistribution(attribute.rawAttribute))
                }).groupBy({_._1}).map({case (className,values) => (className, values.map({_._2}))}).toMap
                MeanCharacterCosineSimilarityFeatureExtractor(classes, auxData)
            }, {
                val auxData = attrsWithNames.map({case attr =>
                    val attrId = attr.rawAttribute.id
                    val label = labels.findLabel(attrId)
                    (label, (attr.preprocessedDataMap("attribute-name-tokenized").asInstanceOf[List[String]]))
                }).groupBy({_._1}).map({case (className, values) => (className, values.map({_._2}))}).toMap
                JCNMinWordNetDistFromClassExamplesFeatureExtractor(classes, auxData, 5)
            },  {
                val auxData = attrsWithNames.map({case attr =>
                    val attrId = attr.rawAttribute.id
                    val label = labels.findLabel(attrId)
                    (label, (attr.preprocessedDataMap("attribute-name-tokenized").asInstanceOf[List[String]]))
                }).groupBy({_._1}).map({case (className, values) => (className, values.map({_._2}))}).toMap
                LINMinWordNetDistFromClassExamplesFeatureExtractor(classes, auxData, 5)
            }
        )

        featureExtractors1 ++ featureExtractors2
    }


    def save(attributes: List[PreprocessedAttribute],
             featuresList: List[List[Any]],
             header: String,
             labels: SemanticTypeLabels,
             path: String) = {
        var out = new PrintWriter(new File(path))
        out.println(header)

        //write features
        (attributes zip featuresList).foreach({case (attr, features) =>
            val id = attr.rawAttribute.id
            out.println((id :: labels.findLabel(attr.rawAttribute.id) :: features).mkString(","))
        })

        out.close()
    }
}