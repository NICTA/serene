//package au.csiro.data61.matcher.matcher.eval
//
//import org.specs2._
//import au.csiro.data61.matcher.data._
//import au.csiro.data61.matcher.ingestion.loader._
//import au.csiro.data61.matcher.matcher.eval.EvalTypes._
//import au.csiro.data61.matcher.matcher.features._
//
//import scala.io._
//
//class RfKnnSemanticTypeClassifierEvaluationProcessSpec extends mutable.Specification {
//    val classListPath = "dirstruct/semantic_type_classifier/repo/classes/class_list.csv"
//    val rawDataPath = "dirstruct/semantic_type_classifier/repo/raw_data"
//    val labelsPath = "dirstruct/semantic_type_classifier/repo/labels/manual"
//    val minTrainSetProp = 0.2
//    val trainSizeIncrement = 0.2
//    val testSetSize = 0.4
//    val numRepetitions = 1
//    val outputPath = "dirstruct/semantic_type_classifier/repo/eval"
//
//    s"""RfKnnSemanticTypeClassifierEvaluation startEvaluation()""" should {
//        "perform evaluation using increasing training set size" in {
//            val process = RfKnnSemanticTypeClassifierEvaluationProcess()
//            val classes: List[String] = Source.fromFile(classListPath).getLines.toList :+ "unknown"
//            process.startEvaluation(
//                classes,
//                rawDataPath,
//                labelsPath,
//                minTrainSetProp,
//                trainSizeIncrement,
//                testSetSize,
//                numRepetitions,
//                "ResampleToMean",
//                FeatureSettings.load("src/test/resources/config/features_config.json"),
//                Some("src/test/resources/config/cost_matrix.txt"),
//                outputPath)
//            1 mustEqual 1
//        }
//    }
//}