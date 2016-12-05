package au.csiro.data61.matcher.matcher.eval.metrics

trait EvaluationMetrics {
}

case class StandardEvaluationMetrics(val confusionMatrix: ConfusionMatrix) extends EvaluationMetrics {
    def precision = {
        val denom = (confusionMatrix.numTruePositive+confusionMatrix.numFalsePositive).toDouble
        if(denom == 0) 0 else confusionMatrix.numTruePositive.toDouble / denom
    }
    def recall = {
        val denom = (confusionMatrix.numTruePositive+confusionMatrix.numFalseNegative).toDouble
        if(denom == 0) 0 else confusionMatrix.numTruePositive.toDouble / denom
    }
    def f1 = {
        val denom = (2*confusionMatrix.numTruePositive + confusionMatrix.numFalsePositive + confusionMatrix.numFalseNegative).toDouble
        if(denom == 0) 0 else 2*confusionMatrix.numTruePositive.toDouble / denom
    }
}