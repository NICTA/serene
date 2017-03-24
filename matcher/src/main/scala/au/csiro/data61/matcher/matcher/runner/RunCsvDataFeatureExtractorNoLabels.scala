package au.csiro.data61.matcher.matcher.runner

import au.csiro.data61.matcher.data._
import au.csiro.data61.matcher.matcher.features._
import au.csiro.data61.matcher.matcher.eval.datasetutils._
import au.csiro.data61.matcher.ingestion.loader._

import collection.parallel.ForkJoinTasks.defaultForkJoinPool._

import java.io._

object RunCsvDataFeatureExtractorNoLabels {
    val usageMessage = """Usage:
                         #    java RunFeatureExtractor <inputDataDir> <outputDir>
                       """.stripMargin('#')

    def main(args: Array[String]) = {
        if(args.size < 2) {
            println(usageMessage)
        } else {
            //create output folder if it doesn't exist
            val inputDir = args(0)
            val outputDir = args(1)

            val dirFile = new File(outputDir)
            if(!dirFile.exists) {
                dirFile.mkdir()
            }

            //specify all fetures we want
            val featureExtractors = List(
                NumUniqueValuesFeatureExtractor(),
                PropUniqueValuesFeatureExtractor(),
                PropMissingValuesFeatureExtractor(),
                DataTypeFeatureExtractor(),
                NumericalCharRatioFeatureExtractor(),
                WhitespaceRatioFeatureExtractor(),
                TextStatsFeatureExtractor(),
                NumberTypeStatsFeatureExtractor(),
                DiscreteTypeFeatureExtractor(),
                EntropyForDiscreteDataFeatureExtractor(),
                PropAlphaCharsFeatureExtractor(),
                PropEntriesWithAtSign(),
                PropEntriesWithHyphen(),
                PropRangeFormat(),
                DatePatternFeatureExtractor()
            )

            //load dataset
            val d = new java.io.File(inputDir)
            d.list.par.foreach({name => 
                val dataset = CsvDataLoader().load(s"$inputDir/$name")
                val tableFile = new File(s"$outputDir/$name")
                if(!tableFile.exists) {
                    var out = new PrintWriter(tableFile)
                    val preprocessedAttrs = DataPreprocessor().preprocess(DataModel.getAllAttributes(dataset))
                    val features: List[List[Any]] = FeatureExtractionProcess().extractFeatures(featureExtractors, preprocessedAttrs)

                    //write headers
                    out.println(("id" :: featureExtractors.flatMap({
                        case fe: SingleFeatureExtractor => List(fe.getFeatureName)
                        case gfe: GroupFeatureExtractor => gfe.getFeatureNames
                    })).mkString(","))

                    //write features
                    (preprocessedAttrs zip features).foreach({case (attr, features) =>
                        val id = attr.rawAttribute.id
                        out.println((id :: features).mkString(","))
                    })

                    out.close()
                } else {
                    println("This file exists already: " + tableFile.getName)
                }
            })

            println("DONE!")
        }
    }
}