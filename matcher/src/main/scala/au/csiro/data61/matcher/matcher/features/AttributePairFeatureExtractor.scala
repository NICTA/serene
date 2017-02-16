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
import au.csiro.data61.matcher.nlptools.distance._
import au.csiro.data61.matcher.nlptools.parser._
import au.csiro.data61.matcher.nlptools.tokenizer._

import edu.cmu.lti.ws4j._

trait AttributePairFeatureExtractor {
    def getNames(): List[String]
    def computeFeature(attribute1: PreprocessedAttribute, attribute2: PreprocessedAttribute): List[Any]
}

case class RatioUniqueValuesDifferenceFeatureExtractor() extends AttributePairFeatureExtractor {
    override def getNames(): List[String] = List("numUniqueValsDiff")

    override def computeFeature(attribute1: PreprocessedAttribute,
                                attribute2: PreprocessedAttribute
                               ): List[Any] = {
        val a1Ratio = attribute1.preprocessedDataMap("num-unique-vals").asInstanceOf[Int].toDouble / attribute1.rawAttribute.values.size.toDouble
        val a2Ratio = attribute2.preprocessedDataMap("num-unique-vals").asInstanceOf[Int].toDouble / attribute2.rawAttribute.values.size.toDouble
        List(Math.abs(a1Ratio - a2Ratio))
    }
}

/**
 *  Data type indicator fields
 **/
case class DataTypeEqualityFeatureExtractor() extends AttributePairFeatureExtractor {
    override def getNames(): List[String] = List("bothFloatType", "bothIntType", "bothStringType", "bothUnknownType")

    override def computeFeature(attribute1: PreprocessedAttribute, attribute2: PreprocessedAttribute): List[Any] = {
        val keys = List(
            "inferred-type-float",
            "inferred-type-integer",
            "inferred-type-boolean",
            "inferred-type-date",
            "inferred-type-time",
            "inferred-type-datetime",
            "inferred-type-string"
        )
        
        keys.map {
            k => {
                if(attribute1.preprocessedDataMap(k)
                  .asInstanceOf[Boolean] && attribute2.preprocessedDataMap(k).asInstanceOf[Boolean]) {
                    1.0
                } else {
                    0.0
                }
            }
        }
    }
}


/**
 *  String distance features.
 **/
case class StringEqualityFeatureExtractor() extends AttributePairFeatureExtractor {
    override def getNames(): List[String] = List("stringEquality")

    override def computeFeature(attribute1: PreprocessedAttribute, attribute2: PreprocessedAttribute): List[Any] = {
        val attr1 = attribute1.rawAttribute
        val attr2 = attribute2.rawAttribute
        (attr1.metadata, attr2.metadata) match {
            case (Some(Metadata(name1,_)), Some(Metadata(name2,_))) => if(name1 equalsIgnoreCase name2) List(1.0) else List(0.0)
            case _ => List(1.0)
        }
    }
}
case class NeedlemanWunschFeatureExtractor() extends AttributePairFeatureExtractor {
    override def getNames(): List[String] = List("needlemanWunschDistance")

    override def computeFeature(attribute1: PreprocessedAttribute, attribute2: PreprocessedAttribute): List[Any] = {
        val attr1 = attribute1.rawAttribute
        val attr2 = attribute2.rawAttribute
        (attr1.metadata, attr2.metadata) match {
            case (Some(Metadata(name1,_)), Some(Metadata(name2,_))) => List(OntoSimDistanceMetrics.computeDistance("NeedlemanWunschDistance")(name1,name2))
            case _ => List(1.0)
        }
    }
}
case class NgramDistanceFeatureExtractor() extends AttributePairFeatureExtractor {
    override def getNames(): List[String] = List("ngramDistance")

    override def computeFeature(attribute1: PreprocessedAttribute, attribute2: PreprocessedAttribute): List[Any] = {
        val attr1 = attribute1.rawAttribute
        val attr2 = attribute2.rawAttribute
        (attr1.metadata, attr2.metadata) match {
            case (Some(Metadata(name1,_)), Some(Metadata(name2,_))) => List(OntoSimDistanceMetrics.computeDistance("NGramDistance")(name1,name2))
            case _ => List(1.0)
        }
    }
}
case class JaroMeasureFeatureExtractor() extends AttributePairFeatureExtractor {
    override def getNames(): List[String] = List("jaroMeasureDistance")

    override def computeFeature(attribute1: PreprocessedAttribute, attribute2: PreprocessedAttribute): List[Any] = {
        val attr1 = attribute1.rawAttribute
        val attr2 = attribute2.rawAttribute
        (attr1.metadata, attr2.metadata) match {
            case (Some(Metadata(name1,_)), Some(Metadata(name2,_))) => List(OntoSimDistanceMetrics.computeDistance("JaroMeasure")(name1,name2))
            case _ => List(1.0)
        }
    }
}

/**
 *  Semantic features.
 **/
case class WordNetDistanceFeatureExtractor() extends AttributePairFeatureExtractor {
  val wordNetDistMetric = WordNetDistanceMetric()

  override def getNames(): List[String] = List("wordnetDistance")

  override def computeFeature(attribute1: PreprocessedAttribute, attribute2: PreprocessedAttribute): List[Any] = {
        val attr1 = attribute1.rawAttribute
        val attr2 = attribute2.rawAttribute
        (attr1.metadata, attr2.metadata) match {
      case (Some(Metadata(name1,_)), Some(Metadata(name2,_))) => List(wordNetDistMetric.computeDistance(name1,name2))
      case _ => List(1.0)
    }
  }
}

/**
 *  WS4J wordnet distance features
 **/
abstract class WS4JWordNetFeatureExtractor(val algorithm : (String,String) => Double) extends AttributePairFeatureExtractor {
    override def computeFeature(attribute1: PreprocessedAttribute, attribute2: PreprocessedAttribute): List[Any] = {
        val wordSet1 = attribute1.preprocessedDataMap("attribute-name-tokenized").asInstanceOf[List[String]]
        val wordSet2 = attribute2.preprocessedDataMap("attribute-name-tokenized").asInstanceOf[List[String]]

        if(wordSet1.size == 0 || wordSet2.size == 0) List(-1)
        else List(computeFeatureBetweenCompoundedWords(wordSet1,wordSet2))
    }

   def computeFeatureForSimpleWords(s1: String, s2: String): Double = {
        if(s1.equalsIgnoreCase(s2)) 1.0
        else {
            val word1 = s1 + "#n"
            val word2 = s2 + "#n"
            algorithm(word1,word2)
        }
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
            val minMatch = set2.map({case x => (x,featureValuesMap((head,x)))}).maxBy(_._2)
            List((head, minMatch._1, minMatch._2)) ++ findBestAlignment(set1 - head, set2, featureValuesMap)
        }
    }    
}
case class JCNWordNetFeatureExtractor() extends WS4JWordNetFeatureExtractor(WS4J.runJCN _) {
    override def getNames(): List[String] = List("ws4jJCNWordNetSimilarity")
}
case class LINWordNetFeatureExtractor() extends WS4JWordNetFeatureExtractor(WS4J.runLIN _) {
    override def getNames(): List[String] = List("ws4jLINWordNetSimilarity")
}



case class TermCosineSimilarityFeatureExtractor() extends AttributePairFeatureExtractor {
    override def getNames(): List[String] = List("termFrequencyCosineSimilarity")

    override def computeFeature(attribute1: PreprocessedAttribute, attribute2: PreprocessedAttribute): List[Any] = {
        if(attribute1.preprocessedDataMap("inferred-type-string").asInstanceOf[Boolean] && attribute2.preprocessedDataMap("inferred-type-string").asInstanceOf[Boolean]) {
            val attr1Tf = attribute1.preprocessedDataMap("normalised-term-frequency-vector").asInstanceOf[Map[String,Double]]
            val attr2Tf = attribute2.preprocessedDataMap("normalised-term-frequency-vector").asInstanceOf[Map[String,Double]]

            val attr1Terms = attr1Tf.keys.toSet
            val attr2Terms = attr2Tf.keys.toSet
            val unionTerms = (attr1Terms ++ attr2Terms).toList

            //compute crossproduct of unit vectors
            List(unionTerms.map({case k => attr1Tf.getOrElse(k,0.0) * attr2Tf.getOrElse(k,0.0)}).sum)
        } else {
            List(-1.0) //default to -1 if this feature is not applicable
        }
    }
}

case class CharacterCosineSimilarityFeatureExtractor() extends AttributePairFeatureExtractor {
    override def getNames(): List[String] = List("characterFrequencyCosineSimilarity")

    override def computeFeature(attribute1: PreprocessedAttribute, attribute2: PreprocessedAttribute): List[Any] = {
        if(attribute1.preprocessedDataMap("inferred-type-string").asInstanceOf[Boolean] && attribute2.preprocessedDataMap("inferred-type-string").asInstanceOf[Boolean]) {

            val attr1Tf = attribute1.preprocessedDataMap("normalised-char-frequency-vector").asInstanceOf[Map[Char,Double]]
            val attr2Tf = attribute2.preprocessedDataMap("normalised-char-frequency-vector").asInstanceOf[Map[Char,Double]]

            val attr1Terms = attr1Tf.keys.toSet
            val attr2Terms = attr2Tf.keys.toSet
            val unionTerms = (attr1Terms ++ attr2Terms).toList

            //compute crossproduct of unit vectors
            val crossprod = unionTerms.map({case k => attr1Tf.getOrElse(k,0.0) * attr2Tf.getOrElse(k,0.0)}).sum
            if(java.lang.Double.isNaN(crossprod)) {
                List(0)
            } else {
                List(crossprod)
            }
        } else {
            List(-1.0) //default to -1 if this feature is not applicable
        }
    }
}


/**
 *  Metadata similarity features.
 **/
// case class DataTypeEqualityFeatureExtractor extends AttributePairFeatureExtractor {
//     override def getNames(): List[String] = List("dataTypeEquality")

//     var cache = scala.collection.mutable.Map[String,String]()

//  override def computeFeature(attribute1: PreprocessedAttribute, attribute2: PreprocessedAttribute): List[Any] = {
//      val a1ValSamples = scala.util.Random.shuffle(attribute1.rawAttribute.values).take(100)
//      val a2ValSamples = scala.util.Random.shuffle(attribute2.rawAttribute.values).take(100)

//         val attribute1Type = getAttributeType(attribute1.rawAttribute)
//         val attribute2Type = getAttributeType(attribute2.rawAttribute)
//         if(attribute1Type == "UNKNOWN" || attribute2Type == "UNKNOWN") List(0)
//         else if(attribute1Type == attribute2Type) List(1)
//         else List(0)
//  }

//  def getAttributeType(attribute: Attribute): String = {
//      cache.getOrElseUpdate(attribute.id, {
//             val sample = scala.util.Random.shuffle(attribute.values).take(100)
//             DataTypeParser.inferDataTypes(sample).groupBy(identity)
//              .map({case (key,value) => (key,value.size)})
//              .foldLeft(("UNKNOWN",0))({
//                  case ((type1,count1),(type2,count2)) => if(count1 > count2) (type1,count1) else (type2,count2)
//               })._1
//          })
//  }
// }


case class NumericalCharRatioDifferenceFeatureExtractor() extends AttributePairFeatureExtractor {
    override def getNames(): List[String] = List("numCharRatioDiff")

    override def computeFeature(attribute1: PreprocessedAttribute, attribute2: PreprocessedAttribute): List[Any] = {
        val numRegex = "[0-9]".r
        val ratios1 = attribute1.rawAttribute.values.map({
            case s if s.length > 0 => numRegex.findAllIn(s).size.toDouble / s.size.toDouble
            case _ => 0
        })
        val ratio1 = ratios1.sum.toDouble / ratios1.size.toDouble

        val ratios2 = attribute2.rawAttribute.values.map({
            case s if s.length > 0 => numRegex.findAllIn(s).size.toDouble / s.size.toDouble
            case _ => 0
        })
        val ratio2 = ratios2.sum.toDouble / ratios2.size.toDouble


        List(Math.abs(ratio1 - ratio2))
    }
}


case class WhitespaceRatioDifferenceFeatureExtractor() extends AttributePairFeatureExtractor {
    override def getNames(): List[String] = List("whitespaceRatio")

    override def computeFeature(attribute1: PreprocessedAttribute, attribute2: PreprocessedAttribute): List[Any] = {
        val numRegex = """\s""".r
        val ratios1 = attribute1.rawAttribute.values.map({
            case s if s.length > 0 => numRegex.findAllIn(s).size.toDouble / s.size.toDouble
            case _ => 0
        })
        val ratio1 = ratios1.sum.toDouble / ratios1.size

        val ratios2 = attribute2.rawAttribute.values.map({
            case s if s.length > 0 => numRegex.findAllIn(s).size.toDouble / s.size.toDouble
            case _ => 0
        })
        val ratio2 = ratios2.sum.toDouble / ratios2.size

        List(Math.abs(ratio1 - ratio2))
    }
}

case class TextStatsDifferencesFeatureExtractor() extends AttributePairFeatureExtractor {
    override def getNames(): List[String] = List("stringLengthMeanDiff","stringLengthMedianDiff",
                                                 "stringLengthModeDiff","stringLengthMinDiff","stringLengthMaxDiff")

    override def computeFeature(attribute1: PreprocessedAttribute, attribute2: PreprocessedAttribute): List[Any] = {
        val stats1 = attribute1.preprocessedDataMap("string-length-stats").asInstanceOf[List[Double]]
        val stats2 = attribute2.preprocessedDataMap("string-length-stats").asInstanceOf[List[Double]]
        (stats1 zip stats2).map({case (a,b) => Math.abs(a-b)}).toList
    }    
}

/**
 * Information Theoretic features
 */
case class EntropyDifferenceForDiscreteDataFeatureExtractor() extends AttributePairFeatureExtractor {
    override def getNames(): List[String] = List("entropyDifference")

    override def computeFeature(attribute1: PreprocessedAttribute, attribute2: PreprocessedAttribute): List[Any] = {
        val entropy1 = attribute1.preprocessedDataMap("entropy").asInstanceOf[Double]
        val entropy2 = attribute2.preprocessedDataMap("entropy").asInstanceOf[Double]
        if(entropy1 != -1 && entropy2 != -1) {
            List(Math.abs(entropy1 - entropy2))
        } else {
            List(10000) //some arbitrary large number
        }
    }
}