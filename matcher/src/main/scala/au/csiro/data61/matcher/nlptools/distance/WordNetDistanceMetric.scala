package au.csiro.data61.matcher.nlptools.distance

import au.csiro.data61.matcher.nlptools.semantic._

import scala.math._

case class WordNetDistanceMetric() extends StringDistanceMetric {
    val synonymProvider = WordNetSynProvider()

    def computeDistance(s1: String, s2: String): Double = {
        computeWordNetGraphDistance(Set(s1),Set(s2))
    }

    def computeWordNetGraphDistance(set1: Set[String], 
                                    set2: Set[String], 
                                    currDist: Int = 0, 
                                    excludeWords: Set[String] = Set(),
                                    expandBoth: Boolean = false, 
                                    maxDistance: Int = 5): Double = {
        if(currDist == maxDistance) return 1.0
        else if((set1 & set2).size > 0) {
            return 1.0 / 1.0-Math.pow(1.5, -currDist)
        }
        else return {
            val extExcludeWords = excludeWords ++ set1 
        val newWordSet = extExcludeWords.foldLeft(set1.flatMap(synonymProvider.findSynonyms))((newSet,oldWord) => newSet-oldWord)
            computeWordNetGraphDistance(newWordSet, if(expandBoth) set2.flatMap(synonymProvider.findSynonyms) else set2, currDist+1, extExcludeWords)
        }
    }
}