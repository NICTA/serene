package au.csiro.data61.matcher.nlptools.distance

trait StringDistanceMetric {
    def computeDistance(str1: String, str2: String): Double
}

case class LCSubsequenceDistanceMetric() extends StringDistanceMetric {
    def computeDistance(s1: String, s2: String): Double = 1.0 - (2.0*computeLCSubsequence(s1,s2)/(s1.length+s2.length).toDouble)

    def computeLCSubsequence(s1: String, s2: String): Double = {
        var prevLens = Array.fill[Int](s2.length+1)(0)
        var prevLen = 0
        for(s1Idx <- 1 to s1.length) {
            var prevLen = 0
            for(s2Idx <- 1 to s2.length) {
                var maxScore = 0

                if(s1.charAt(s1Idx-1) == s2.charAt(s2Idx-1)) {
                    maxScore = 1 + prevLens(s2Idx-1)
                } else {
                    maxScore = Math.max(prevLens(s2Idx-1),Math.max(prevLen,prevLens(s2Idx)))
                }

                prevLens(s2Idx-1) = prevLen
                prevLen = maxScore
            }
            prevLens(s2.length) = prevLen
        }

        return prevLens(s2.length)
    }
}