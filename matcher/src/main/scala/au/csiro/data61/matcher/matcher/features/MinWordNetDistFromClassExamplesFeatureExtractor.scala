/**
  * Copyright (C) 2015-2016 Data61, Commonwealth Scientific and Industrial Research Organisation (CSIRO).
  * See the LICENCE.txt file distributed with this work for additional
  * information regarding copyright ownership.
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

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
    def getGroupName(): String = "min-wordnet-jcn-distance-from-class-examples"
}
case class JCNMinWordNetDistFromClassExamplesFeatureExtractor(classList: List[String],
                                                              pool: Map[String,List[TokenizedWords]],
                                                              maxComparisons: Int
                                                             ) extends HeaderGroupFeatureExtractor {

  override def getGroupName(): String = JCNMinWordNetDistFromClassExamplesFeatureExtractor.getGroupName

  override def getFeatureNames(): List[String] =
    classList.map {
      className => "min-wordnet-jcn-distance-" + className
  }


  override def computeSimpleFeatures(attribute: SimpleAttribute): List[Double] = {
    val attrNameTokens = attribute
      .attributeNameTokenized

    classList.map {
      className =>
        val examples = pool.getOrElse(className, List())
        val examplesSubset = if(examples.size > maxComparisons) {
          new Random(10857171).shuffle(examples).take(maxComparisons)
        } else {
          examples
        }

        if(examplesSubset.isEmpty) {
          -1
        } else {
          examplesSubset.map {
            colNameTokens =>
              if(attrNameTokens.isEmpty || colNameTokens.isEmpty) {
                -1
              } else {
                computeFeatureBetweenCompoundedWords(attrNameTokens,colNameTokens)
              }
          }.min
        }
    }
  }

  override def computeFeatures(attribute: PreprocessedAttribute): List[Double] = {
    val attrNameTokens = attribute
      .preprocessedDataMap("attribute-name-tokenized")
      .asInstanceOf[List[String]]

    classList.map {
      className =>
        val examples = pool.getOrElse(className, List())
        val examplesSubset = if(examples.size > maxComparisons) {
            new Random(10857171).shuffle(examples).take(maxComparisons)
        } else {
            examples
        }

        if(examplesSubset.isEmpty) {
            -1
        } else {
          examplesSubset.map {
            colNameTokens =>
              if(attrNameTokens.isEmpty || colNameTokens.isEmpty) {
                -1
              } else {
                computeFeatureBetweenCompoundedWords(attrNameTokens,colNameTokens)
              }
          }.min
        }
    }
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
    val featureValuesMap = (for(i <- biggerSet; j <- smallerSet) yield ((i,j),
      computeFeatureForSimpleWords(i,j))).toMap
    val bestAlignment = findBestAlignment(biggerSet.toSet, smallerSet.toSet, featureValuesMap)
    bestAlignment.map(_._3).sum.toDouble / bestAlignment.size.toDouble
  }

  def findBestAlignment(set1: Set[String], set2: Set[String], featureValuesMap: Map[(String,String),Double]): List[(String,String,Double)] = {
    if(set1.isEmpty) {
      List()
    } else {
      val head = set1.head
      val bestMatch = set2.map{
        x => (x,featureValuesMap((head,x)))
      }.minBy(_._2)
      List((head, bestMatch._1, bestMatch._2)) ++ findBestAlignment(set1 - head, set2, featureValuesMap)
    }
  }
}



object LINMinWordNetDistFromClassExamplesFeatureExtractor {
    def getGroupName(): String = "min-wordnet-lin-distance-from-class-examples"
}
case class LINMinWordNetDistFromClassExamplesFeatureExtractor(classList: List[String],
                                                              pool: Map[String,List[TokenizedWords]],
                                                              maxComparisons: Int
                                                             ) extends HeaderGroupFeatureExtractor {

  override def getGroupName(): String =
    LINMinWordNetDistFromClassExamplesFeatureExtractor.getGroupName
  override def getFeatureNames(): List[String] =
    classList
      .map {
        className => "min-wordnet-lin-distance-" + className
      }


  override def computeSimpleFeatures(attribute: SimpleAttribute): List[Double] = {
    val attrNameTokens = attribute
      .attributeNameTokenized

    classList.map {
      className =>
        val examples = pool.getOrElse(className, List())
        val examplesSubset = if(examples.size > maxComparisons) {
          new Random(10857171).shuffle(examples).take(maxComparisons)
        } else {
          examples
        }

        if(examplesSubset.isEmpty) {
          -1
        } else {
          examplesSubset.map {
            colNameTokens =>
              if(attrNameTokens.isEmpty || colNameTokens.isEmpty) {
                -1
              } else {
                computeFeatureBetweenCompoundedWords(attrNameTokens,colNameTokens)
              }
          }.min
        }
    }
  }

  override def computeFeatures(attribute: PreprocessedAttribute): List[Double] = {
    val attrNameTokens = attribute
      .preprocessedDataMap("attribute-name-tokenized")
      .asInstanceOf[List[String]]

    classList.map {
      className =>
        val examples = pool.getOrElse(className, List())
        val examplesSubset = if(examples.size > maxComparisons) {
          new Random(10857171).shuffle(examples).take(maxComparisons)
        } else {
          examples
        }

        if(examplesSubset.isEmpty) {
            -1
        } else {
          examplesSubset.map {
            colNameTokens =>
              if(attrNameTokens.isEmpty || colNameTokens.isEmpty) {
                -1
              } else {
                computeFeatureBetweenCompoundedWords(attrNameTokens,colNameTokens)
              }
            }.min
        }
    }
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
      val featureValuesMap = (for(i <- biggerSet; j <- smallerSet) yield ((i,j),
        computeFeatureForSimpleWords(i,j))).toMap
      val bestAlignment = findBestAlignment(biggerSet.toSet, smallerSet.toSet, featureValuesMap)
      bestAlignment.map(_._3).sum.toDouble / bestAlignment.size.toDouble
  }

  def findBestAlignment(set1: Set[String],
                        set2: Set[String],
                        featureValuesMap: Map[(String,String),Double]
                       ): List[(String,String,Double)] = {
      if(set1.isEmpty) {
        List()
      }
      else {
        val head = set1.head
        val bestMatch = set2.map({case x => (x,featureValuesMap((head,x)))}).minBy(_._2)
        List((head, bestMatch._1, bestMatch._2)) ++ findBestAlignment(set1 - head, set2, featureValuesMap)
      }
  }
}