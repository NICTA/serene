package au.csiro.data61.matcher.matcher.eval.metrics

import scala.util._

object EvaluationMetricsUtil {
    def computeF1(predictionResults: List[(Double,Double)], threshold: Double): Double = {
        val confMatrixVals = predictionResults.map({
            case (pred, label) => (pred >= threshold, label == 1.0)
        }).foldLeft((0,0,0,0))((counts, predSignAndGt) => (counts, predSignAndGt) match {
            case ((tp,fp,tn,fn), (true, true))   => (tp+1,fp,tn,fn)
            case ((tp,fp,tn,fn), (true, false))  => (tp,fp+1,tn,fn)
            case ((tp,fp,tn,fn), (false, false)) => (tp,fp,tn+1,fn)
            case ((tp,fp,tn,fn), (false, true))  => (tp,fp,tn,fn+1)
        })
        StandardEvaluationMetrics(ConfusionMatrix(confMatrixVals._1, confMatrixVals._2, confMatrixVals._3, confMatrixVals._4)).f1
    }
}