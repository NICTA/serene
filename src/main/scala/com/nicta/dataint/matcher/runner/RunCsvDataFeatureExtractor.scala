package com.nicta.dataint.matcher.runner

import com.nicta.dataint.data._
import com.nicta.dataint.matcher.features._
import com.nicta.dataint.matcher.eval.datasetutils._
import com.nicta.dataint.ingestion.loader._

import collection.parallel.ForkJoinTasks.defaultForkJoinPool._

import java.io._

object RunCsvDataFeatureExtractor {
    val usageMessage = """Usage:
                         #    java RunFeatureExtractor <inputDataDir> <labelsPath> <outputDir>
                       """.stripMargin('#')

    def main(args: Array[String]) = {
        if(args.size < 2) {
            println(usageMessage)
        } else {
            //create output folder if it doesn't exist
            val inputDir = args(0)
            val labelsPath = args(1)
            val outputDir = args(2)

            val dirFile = new File(outputDir)
            if(!dirFile.exists) {
                dirFile.mkdir()
            }

            //load labels
            val labels = SemanticTypeLabelsLoader().load(labelsPath)

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
                val dataset = CSVDataLoader().load(s"$inputDir/$name")
                val tableFile = new File(s"$outputDir/$name")
                if(!tableFile.exists) {
                    var out = new PrintWriter(tableFile)
                    val preprocessedAttrs = DataPreprocessor().preprocess(DataModel.getAllAttributes(dataset))
                    val features: List[List[Any]] = FeatureExtractionProcess().extractFeatures(featureExtractors, preprocessedAttrs)

                    //write headers
                    out.println(("id,label" :: featureExtractors.flatMap({
                        case fe: SingleFeatureExtractor => List(fe.getFeatureName)
                        case gfe: GroupFeatureExtractor => gfe.getFeatureNames
                    })).mkString(","))

                    //write features
                    (preprocessedAttrs zip features).foreach({case (attr, features) =>
                        val id = attr.rawAttribute.id
                        out.println((id :: labels.findLabel(attr.rawAttribute.id) :: features).mkString(","))
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