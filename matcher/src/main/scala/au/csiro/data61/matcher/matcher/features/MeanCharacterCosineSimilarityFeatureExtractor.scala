package com.nicta.dataint.matcher.features

import com.nicta.dataint.data._
import com.nicta.dataint.matcher._
import com.nicta.dataint.nlptools.distance._


//classExamplesMap contains a map from class -> a list of examples for that class.
//each example has a character distribution vector stored as a map.
object MeanCharacterCosineSimilarityFeatureExtractor {
    def getGroupName() = "mean-character-cosine-similarity-from-class-examples"
}
case class MeanCharacterCosineSimilarityFeatureExtractor(classList: List[String], classExamplesMap: Map[String,List[Map[Char,Double]]]) extends GroupFeatureExtractor {
    override def getGroupName() = MeanCharacterCosineSimilarityFeatureExtractor.getGroupName
    override def getFeatureNames(): List[String] = classList.map({className => s"mean-charcosinedist-$className"}).toList
    
    override def computeFeatures(attribute: PreprocessedAttribute): List[Double] = {
        val charDist = computeCharDistribution(attribute.rawAttribute)

        classList.map({className => 
            classExamplesMap.get(className).map({
                case classExamples => classExamples.map({
                    case classExampleCharDist => computeCosineDistanceDistance(charDist, classExampleCharDist)
                }).sum / classExamples.size.toDouble
            }).getOrElse(Double.MaxValue)
        }).toList
    }

    def computeCosineDistanceDistance(attr1Tf: Map[Char,Double], attr2Tf: Map[Char,Double]): Double = {
        val attr1Terms = attr1Tf.keys.toSet
        val attr2Terms = attr2Tf.keys.toSet
        val unionTerms = (attr1Terms ++ attr2Terms).toList

        //compute crossproduct of unit vectors
        val crossprod = unionTerms.map({case k => attr1Tf.getOrElse(k,0.0) * attr2Tf.getOrElse(k,0.0)}).sum
        if(java.lang.Double.isNaN(crossprod)) {
            0
        } else {
            crossprod
        }
    }

    def computeCharDistribution(attribute: Attribute): Map[Char,Double] = {
        val counts = scala.collection.mutable.Map[Char, Int]()
        attribute.values.foreach({case v =>
            v.toCharArray.foreach({case c =>
                counts += (c -> (counts.getOrElse(c, 0) + 1))
            })
        })

        if(counts.size > 0) {
            //we downscale the counts vector by 1/maxCount, compute the norm, then upscale by maxCount to prevent overflow
            val maxCount = counts.values.max
            val dscaledCounts = counts.map({case (k,v) => (k,v.toDouble/maxCount.toDouble)})
            val norm = Math.sqrt((dscaledCounts.values.map({case x => x*x}).sum)) * maxCount
            val normCf = counts.keys.map({case x => (x, if(norm != 0) counts(x).toDouble/norm else 0)}).toMap

            val length = Math.abs(Math.sqrt(normCf.values.map({x=>x*x}).sum) - 1.0)
            assert(Math.abs(Math.sqrt(normCf.values.map({x=>x*x}).sum) - 1.0) <= 0.00005, 
                "length of char freq vector is " + length 
                + "\nnorm: " + norm
                + "\ncounts: " + counts 
                + "\ncounts^2: " + (counts.values.map({case x => x*x})))
            normCf
        } else {
            Map()
        }
    }
}