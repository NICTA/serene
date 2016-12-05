
package au.csiro.data61.matcher.matcher.features

import au.csiro.data61.matcher.data._
import au.csiro.data61.matcher.matcher._
import au.csiro.data61.matcher.nlptools.distance._
import au.csiro.data61.matcher.matcher.features.ExampleBasedFeatureExtractorTypes._

import scala.util._

import edu.cmu.lti.ws4j._

package object ExampleBasedFeatureExtractorTypes {
    type TokenizedWords = List[String]
}


object JCNMinWordNetDistFromClassExamplesFeatureExtractor {
    def getGroupName() = "min-wordnet-jcn-distance-from-class-examples"
}
case class JCNMinWordNetDistFromClassExamplesFeatureExtractor(
    classList: List[String], 
    pool: Map[String,List[TokenizedWords]],
    maxComparisons: Int) extends GroupFeatureExtractor {

    override def getGroupName() = JCNMinWordNetDistFromClassExamplesFeatureExtractor.getGroupName
    override def getFeatureNames(): List[String] = classList.map({className => "min-wordnet-jcn-distance-" + className}).toList

    override def computeFeatures(attribute: PreprocessedAttribute): List[Double] = {
        val attrNameTokens = attribute.preprocessedDataMap("attribute-name-tokenized").asInstanceOf[List[String]]
        classList.map({case className =>
            val examples = pool.getOrElse(className, List())
            val examplesSubset = if(examples.size > maxComparisons) {
                (new Random(10857171)).shuffle(examples).take(maxComparisons)
            } else {
                examples
            }

            if(examplesSubset.size == 0) {
                -1
            } else {
                examplesSubset.map({case colNameTokens => 
                    if(attrNameTokens.size == 0 || colNameTokens.size == 0) {
                        -1
                    } else {
                        computeFeatureBetweenCompoundedWords(attrNameTokens,colNameTokens)
                    }
                }).min
            }
        })
    }

    def computeFeatureForSimpleWords(s1: String, s2: String): Double = {
        if(s1.equalsIgnoreCase(s2)) {
            0.0
        } else {
            val word1 = s1 + "#n"
            val word2 = s2 + "#n"
            algorithm(word1,word2)
        }
    }

    
    def algorithm(s1: String, s2: String): Double = {
        val maxValue = 12876699.5
        // JCN scores are in the gives us higher values for similar words.
        // Let's bound the score from 0-1 instead, where 0 is
        // very similar and 1 is very dissimilar.
        1.0 - WS4J.runJCN(s1,s2).toDouble / maxValue
    }

    /**
     *  This function finds the best alignment between the sets of words.
     **/
    def computeFeatureBetweenCompoundedWords(set1: List[String], set2: List[String]): Double = {
        val (smallerSet, biggerSet) = if(set1.size <= set2.size) (set1,set2) else (set2,set1)
        val featureValuesMap = (for(i <- biggerSet; j <- smallerSet) yield ((i,j), computeFeatureForSimpleWords(i,j))).toMap
        val bestAlignment = findBestAlignment(biggerSet.toSet, smallerSet.toSet, featureValuesMap)
        bestAlignment.map(_._3).sum.toDouble / bestAlignment.size.toDouble
    }

    def findBestAlignment(set1: Set[String], set2: Set[String], featureValuesMap: Map[(String,String),Double]): List[(String,String,Double)] = {
        if(set1.size == 0) List()
        else {
            val head = set1.head
            val bestMatch = set2.map({case x => (x,featureValuesMap((head,x)))}).minBy(_._2)
            List((head, bestMatch._1, bestMatch._2)) ++ findBestAlignment(set1 - head, set2, featureValuesMap)
        }
    }
}



object LINMinWordNetDistFromClassExamplesFeatureExtractor {
    def getGroupName() = "min-wordnet-lin-distance-from-class-examples"
}
case class LINMinWordNetDistFromClassExamplesFeatureExtractor(
    classList: List[String], 
    pool: Map[String,List[TokenizedWords]],
    maxComparisons: Int) extends GroupFeatureExtractor {

    override def getGroupName() = LINMinWordNetDistFromClassExamplesFeatureExtractor.getGroupName
    override def getFeatureNames(): List[String] = classList.map({className => "min-wordnet-lin-distance-" + className}).toList

    override def computeFeatures(attribute: PreprocessedAttribute): List[Double] = {
        val attrNameTokens = attribute.preprocessedDataMap("attribute-name-tokenized").asInstanceOf[List[String]]
        classList.map({case className =>
            val examples = pool.getOrElse(className, List())
            val examplesSubset = if(examples.size > maxComparisons) {
                (new Random(10857171)).shuffle(examples).take(maxComparisons)
            } else {
                examples
            }

            if(examplesSubset.size == 0) {
                -1
            } else {
                examplesSubset.map({case colNameTokens => 
                    if(attrNameTokens.size == 0 || colNameTokens.size == 0) {
                        -1
                    } else {
                        computeFeatureBetweenCompoundedWords(attrNameTokens,colNameTokens)
                    }
                }).min
            }
        })
    }

    def computeFeatureForSimpleWords(s1: String, s2: String): Double = {
        if(s1.equalsIgnoreCase(s2)) {
            0.0
        } else {
            val word1 = s1 + "#n"
            val word2 = s2 + "#n"
            algorithm(word1,word2)
        }
    }

    def algorithm(s1: String, s2: String): Double = {
        //LIN scores are in the gives us higher values for similar words
        //and is bounded in the interval [0,1]
        1.0 - WS4J.runLIN(s1,s2).toDouble
    }

    /**
     *  This function finds the best alignment between the sets of words.
     **/
    def computeFeatureBetweenCompoundedWords(set1: List[String], set2: List[String]): Double = {
        val (smallerSet, biggerSet) = if(set1.size <= set2.size) (set1,set2) else (set2,set1)
        val featureValuesMap = (for(i <- biggerSet; j <- smallerSet) yield ((i,j), computeFeatureForSimpleWords(i,j))).toMap
        val bestAlignment = findBestAlignment(biggerSet.toSet, smallerSet.toSet, featureValuesMap)
        bestAlignment.map(_._3).sum.toDouble / bestAlignment.size.toDouble
    }

    def findBestAlignment(set1: Set[String], set2: Set[String], featureValuesMap: Map[(String,String),Double]): List[(String,String,Double)] = {
        if(set1.size == 0) List()
        else {
            val head = set1.head
            val bestMatch = set2.map({case x => (x,featureValuesMap((head,x)))}).minBy(_._2)
            List((head, bestMatch._1, bestMatch._2)) ++ findBestAlignment(set1 - head, set2, featureValuesMap)
        }
    }
}