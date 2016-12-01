package com.nicta.dataint.matcher.runner

import com.nicta.dataint.data._
import com.nicta.dataint.matcher._
import com.nicta.dataint.matcher.eval.datasetutils._
import com.nicta.dataint.matcher.eval.metrics._
import com.nicta.dataint.ingestion.loader._
import com.nicta.dataint.nlptools.distance.LCSubsequenceDistanceMetric
import com.nicta.dataint.matcher.eval._

import com.nicta.dataint.matcher.train._
import com.nicta.dataint.matcher.serializable._
import com.nicta.dataint.matcher.features._

import scala.collection.JavaConverters._
import scala.io._

import java.io._

/**
 *  This class is called by the dirstruct/semantic_type_classifier/evaluate.sh script.  
 *  For more info on the evaluation process, please see the EVALUATION section in
 *  dirstruct/semantic_type_classifier/HOWTO.
 **/
object RunRfKnnSemanticTypeClassifierEvaluation {
    val usageMessage = """Usage:
                         #    java com.nicta.dataint.matcher.runner.RunRfKnnSemanticTypeClassifierEvaluation <raw_data_path> <class_list_path> <labels_path> <eval_results_path> <min_trainset_proportion> <trainset_proportion_increment> <testset_proportion> <num_repetitions> <resampling_strategy> <features_config_file> [cost_matrix_file]
                       """.stripMargin('#')

    def main(args: Array[String]) = {
        if(args.size < 10) {
            println("Not enough arguments.")
            printUsage()
        } else {
            val rawDataPath = args(0)
            val classListPath = args(1)
            val labelsPath = args(2)
            val outputFolder = args(3)
            val minTrainSetProp = args(4).toDouble
            val trainSetPropIncrement = args(5).toDouble
            val testSetProp = args(6).toDouble
            val numRepetitions = args(7).toInt
            val resamplingStrategy = args(8)
            val featuresConfigFile = args(9)
            val costMatrixFile = if(args.size >= 11) Some(args(10)) else None

            val featuresConfig = FeatureSettings.load(featuresConfigFile)
            val process = RfKnnSemanticTypeClassifierEvaluationProcess()
            val rawClassList = Source.fromFile(classListPath).getLines.toList
            val classes: List[String] = if(!rawClassList.contains("unknown")) ("unknown" :: rawClassList) else rawClassList
            process.startEvaluation(
                classes, rawDataPath, 
                labelsPath, minTrainSetProp, 
                trainSetPropIncrement, testSetProp, 
                numRepetitions, resamplingStrategy, 
                featuresConfig, costMatrixFile,
                outputFolder)
        }
    }

    def printUsage() = {
        println(usageMessage)
    }   
}