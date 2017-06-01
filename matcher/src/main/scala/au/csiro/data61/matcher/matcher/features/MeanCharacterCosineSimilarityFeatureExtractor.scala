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


object MeanCharacterCosineSimilarityFeatureExtractor {
    def getGroupName() = "mean-character-cosine-similarity-from-class-examples"
}

/**
  *
  * @param classList list of classes
  * @param classExamplesMap contains a map from class -> a list of examples for that class.
  *                         each example has a character distribution vector stored as a map.
  */
case class MeanCharacterCosineSimilarityFeatureExtractor(classList: List[String],
                                                         classExamplesMap: Map[String,List[Map[Char,Double]]]
                                                        ) extends GroupFeatureExtractor {

  override def getGroupName(): String =
    MeanCharacterCosineSimilarityFeatureExtractor.getGroupName

  override def getFeatureNames(): List[String] =
    classList.map {
      className =>
        s"mean-charcosinedist-$className"
    }

  override def computeSimpleFeatures(attribute: SimpleAttribute): List[Double]  = {
    val charDist: Map[Char, Double] = attribute.charDist

    classList.map {
      className =>
        classExamplesMap.get(className).map {
          classExamples => classExamples.map {
            classExampleCharDist =>
              computeCosineDistanceDistance(charDist, classExampleCharDist)
          }.sum / classExamples.size.toDouble
        }.getOrElse(Double.MaxValue)
    }
  }

  override def computeFeatures(attribute: PreprocessedAttribute): List[Double] = {
    val charDist: Map[Char, Double] = attribute.preprocessedDataMap
      .getOrElse("normalised-char-frequency-vector",
        computeCharDistribution(attribute.rawAttribute))
      .asInstanceOf[Map[Char,Double]]


    classList.map {
      className =>
        classExamplesMap.get(className).map {
          classExamples => classExamples.map {
            classExampleCharDist =>
              computeCosineDistanceDistance(charDist, classExampleCharDist)
          }.sum / classExamples.size.toDouble
        }.getOrElse(Double.MaxValue)
    }
  }

  def computeCosineDistanceDistance(attr1Tf: Map[Char,Double],
                                    attr2Tf: Map[Char,Double]
                                   ): Double = {
//    val attr1Terms = attr1Tf.keys.toSet
//    val attr2Terms = attr2Tf.keys.toSet
//    val unionTerms = (attr1Terms ++ attr2Terms).toList
//
//    //compute crossproduct of unit vectors
//    val crossprod = unionTerms.map {
//      k => attr1Tf.getOrElse(k,0.0) * attr2Tf.getOrElse(k,0.0)
//    }.sum

    //compute crossproduct of unit vectors
    (attr1Tf ++ ( for ( (k,v) <- attr2Tf ) yield  k -> ( v * attr1Tf.getOrElse(k,0.0) ) ))
      .values.sum

    // TODO: we will not get here NaN... what is this check for?
//    if(java.lang.Double.isNaN(crossprod)) {
//      0
//    } else {
//      crossprod
//    }
  }


  def computeCharDistribution(attribute: Attribute): Map[Char,Double] = {
    // calculate frequencies of chars in the attribute values (rows of the column)
    val counts: Map[Char,Int] = attribute.values
      .flatMap(_.toCharArray)
      .groupBy(_.toChar)
      .mapValues(_.size)

    if(counts.nonEmpty) {
        //we downscale the counts vector by 1/maxCount,
        // compute the norm, then upscale by maxCount to prevent overflow
        val maxCount = counts.values.max
        val dscaled = counts.values
          .map(_.toDouble/maxCount.toDouble) // normalize counts
          .foldLeft(0.0)(_ + Math.pow(_, 2)) // compute sum of squares

        val norm = Math.sqrt(dscaled) * maxCount // the norm of the counts
        val normCf = norm match {
          case 0 => counts.map { case (x, _) => (x,0.0) }
          case normbig =>
            counts.map { case (x, y) => (x, y.toDouble / normbig) }
        }

//        val length = Math.abs(Math.sqrt(normCf.values.foldLeft(0.0)(_ + Math.pow(_, 2))) - 1.0)
//        assert(Math.abs(Math.sqrt(normCf.values.foldLeft(0.0)(_ + Math.pow(_, 2))) - 1.0) <= 0.00005,
//            "length of char freq vector is " + length
//            + "\nnorm: " + norm
//            + "\ncounts: " + counts
//            + "\ncounts^2: " + counts.values.map {x => x*x} )
        normCf
    } else {
        Map()
    }
  }
}