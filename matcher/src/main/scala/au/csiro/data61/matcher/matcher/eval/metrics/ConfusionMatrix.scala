package au.csiro.data61.matcher.matcher.eval.metrics

case class ConfusionMatrix(val numTruePositive: Int, val numFalsePositive: Int,
	                       val numTrueNegative: Int, val numFalseNegative: Int) {
	override def toString() : String = {
		val totalTrue = numTruePositive + numFalseNegative
		val totalNeg = numTrueNegative + numFalsePositive

		var sbuilder = new StringBuilder
        sbuilder ++= "\tPredT\tPredF\tTotals\n"
        sbuilder ++= s"True\t$numTruePositive\t$numFalseNegative\t$totalTrue\n"
        sbuilder ++= s"False\t$numFalsePositive\t$numTrueNegative\t$totalNeg"
        sbuilder.toString
	}
}