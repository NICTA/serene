package au.csiro.data61.matcher.nlptools.distance

import edu.cmu.lti.ws4j._
import au.csiro.data61.matcher.nlptools.tokenizer._

case class WS4JWordNetDistanceMetric(val algorithm: (String,String) => Double = WS4J.runWUP _) extends StringDistanceMetric {

    def computeDistance(s1: String, s2: String): Double = {
        //some string tokenization preprocessing
        val set1 = StringTokenizer.tokenize(s1)
        val set2 = StringTokenizer.tokenize(s2)

        val maximalAssignments = computeBestAlignment(set1,set2)
        maximalAssignments.map({_._3}).sum.toDouble / maximalAssignments.size.toDouble
    }

    def computeDistanceSimpleWords(s1: String, s2: String): Double = {
        if(s1.equalsIgnoreCase(s2)) 0.0
        else {
            //we assume that we're only interested in NOUNS
            val word1 = s1 + "#n"
            val word2 = s2 + "#n"
            1.0 - WS4J.runWUP(word1,word2)
        }
    }

    def computeBestAlignment(set1: List[String], set2: List[String]): List[(String,String,Double)] = {
        val (smallerSet, biggerSet) = if(set1.size <= set2.size) (set1,set2) else (set2,set1)
        val pairwiseDistances = smallerSet.flatMap({case x => 
            biggerSet.map({case y => 
                (x,y,computeDistanceSimpleWords(x,y))
            })
        }).groupBy(_._1)

        pairwiseDistances.keys.map({case k => pairwiseDistances(k).minBy(_._3)}).toList
    }
}