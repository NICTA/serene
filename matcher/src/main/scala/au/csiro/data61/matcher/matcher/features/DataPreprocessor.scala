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
import au.csiro.data61.matcher.nlptools.tokenizer._
import au.csiro.data61.matcher.nlptools.parser._

import scala.util.Random
import com.typesafe.scalalogging.LazyLogging

case class DataPreprocessor() extends LazyLogging {

  val preprocessors: List[AttributePreprocessor] = List(
    AttributeNameTokenizer(),
    AttributeContentTermFrequency(), // it's used only in AttributePairFeatureExtractor
    CharacterDistributionExtractor(),
    DataTypeExtractor(),
    UniqueValuesExtractor(),
    StringLengthStatsExtractor()
  )

  def preprocess(attributes: List[Attribute]): List[PreprocessedAttribute] = {
    logger.info(s"***preprocessing ${attributes.size} attributes...")
    val preprocAttrList = for(i <- attributes.indices) yield {
      val rawAttr = attributes(i)
      if(i % 10 == 0 || i == attributes.size){
        logger.info(s"    processing attribute $i: ${rawAttr.id}")
      }
      preprocess(rawAttr)
    }
    preprocAttrList.toList
  }

  def preprocess(attribute: Attribute): PreprocessedAttribute = {
    val preprocDataMap: Map[String,Any] = preprocessors
      .map(_.preprocess(attribute))
      .foldLeft[Map[String,Any]] (Map())((a,b) => a++b)
    PreprocessedAttribute(attribute, preprocDataMap)
  }
}

abstract class AttributePreprocessor {
  def preprocess(attribute: Attribute): Map[String,Any]
}

case class AttributeNameTokenizer() extends AttributePreprocessor {
  val nameRegex = "([^@]+)@(.+)".r
  override def preprocess(attribute: Attribute): Map[String,Any] = {
    val tokens: List[String] = attribute match {
      case (Attribute(_, Some(Metadata(name,_)), _, _)) => {
        name match {
          case nameRegex(name, _) => StringTokenizer.tokenize(name)
          case x => StringTokenizer.tokenize(x)
        }
      }
      case _ => List()
    }
    Map("attribute-name-tokenized" -> tokens)
  }
}

case class AttributeContentTermFrequency() extends AttributePreprocessor {
  //TODO: remove stop words, stem words
  override def preprocess(attribute: Attribute): Map[String,Any] = {
    val termFrequency: Map[String,Int] =
      attribute match {
        case (Attribute(_, _, data: List[String], _)) => {
          val lines = data.mkString("\n")
            .toLowerCase
            .split("[\n ,]")
            .map(_.trim.replaceAll("[\\.,!?;:]",""))
            .filter(_.nonEmpty)
//          val counts = scala.collection.mutable.Map[String, Int]()
//          lines.foreach {
//            term =>
//              counts += (term -> (counts.getOrElse(term, 0) + 1))
//          }
//          counts
          lines
            .groupBy(identity)
            .mapValues(_.length) // frequency of words
        }
        case _ => Map[String,Int]()
      }
      if(termFrequency.nonEmpty) {
        //we downscale the counts vector by 1/maxCount, compute the norm, then upscale by maxCount to prevent overflow
        val maxCount = termFrequency.values.max
//        val dscaledCounts = termFrequency.map {
//          case (k,v) => (k,v.toDouble/maxCount.toDouble)
//        }
//        val norm = Math.sqrt(dscaledCounts.values.map(x => x*x).sum) * maxCount
//
//        val normTf = termFrequency.keys.map {
//          x => (x,termFrequency(x).toDouble/norm)
//        }.toMap

        val dscaled = termFrequency.values
          .map(_.toDouble/maxCount.toDouble) // normalize counts
          .foldLeft(0.0)(_ + Math.pow(_, 2)) // compute sum of squares

        val norm = Math.sqrt(dscaled) * maxCount // the norm of the counts
        val normTf = norm match {
          case 0 => termFrequency.map { case (x, _) => (x,0.0) }
          case normbig =>
            termFrequency.map { case (x, y) => (x, y.toDouble / normbig) }
        }

        Map("normalised-term-frequency-vector" -> normTf)
      } else {
          Map("normalised-term-frequency-vector" -> Map())
      }
  }
}

case class CharacterDistributionExtractor() extends AttributePreprocessor {
  override def preprocess(attribute: Attribute): Map[String,Any] = {
    val counts: Map[Char,Int] = attribute.values
      .flatMap(_.toCharArray)
      .groupBy(identity)
      .mapValues(_.size)

    if(counts.nonEmpty) {
      //we downscale the counts vector by 1/maxCount, compute the norm, then upscale by maxCount to prevent overflow
      val maxCount = counts.values.max
//      val dscaledCounts = counts.map({case (k,v) => (k,v.toDouble/maxCount.toDouble)})
//      val norm = Math.sqrt((dscaledCounts.values.map({case x => x*x}).sum)) * maxCount
//      val normCf: Map[Char, Double] = counts.keys
//        .map({case x => (x, if(norm != 0) counts(x).toDouble/norm else 0)}).toMap

      val dscaled = counts.values
        .map(_.toDouble/maxCount.toDouble) // normalize counts
        .foldLeft(0.0)(_ + Math.pow(_, 2)) // compute sum of squares
      val norm = Math.sqrt(dscaled) * maxCount // the norm of the counts
      val normCf: Map[Char, Double] = norm match {
        case 0 => counts.map { case (x, _) => (x,0.0) }
        case normbig =>
          counts.map { case (x, y) => (x, y.toDouble / normbig) }
      }
//
//      val length = Math.abs(Math.sqrt(normCf.values.map({x=>x*x}).sum) - 1.0)
//      assert(Math.abs(Math.sqrt(normCf.values.map({x=>x*x}).sum) - 1.0) <= 0.00005,
//        "length of char freq vector is " + length
//          + "\nnorm: " + norm
//          + "\ncounts: " + counts
//          + "\ncounts^2: " + (counts.values.map(x => x*x)))

      Map("normalised-char-frequency-vector" -> normCf)
    } else {
      Map("normalised-char-frequency-vector" -> Map())
    }
  }
}


case class DataTypeExtractor() extends AttributePreprocessor {
  val maxSample = 100

  override def preprocess(attribute: Attribute): Map[String,Any] = {
    val intRegex = """^[+-]?[0-9]+""".r
    val floatRegex = """^[+-]?[0-9]*\.[0-9]+""".r

    val samples = new Random(18371623)
      .shuffle(attribute.values.filter(_.nonEmpty))
      .take(maxSample)
    val maxType = if(samples.nonEmpty) {
        val infDataTypes = DataTypeParser.inferDataTypes(samples)
        val typeCount = infDataTypes.foldLeft(Map[String,Int]())(
          (counts, dtype) => counts + (dtype -> (
            counts.getOrElse(dtype, 0) + 1)))
        typeCount.toList.maxBy(_._2)._1
    } else {
        //default to string
        "String"
    }

    val keys = List(
        ("inferred-type-float", "Float"),
        ("inferred-type-integer", "Integer"),
        ("inferred-type-long", "Long"),
        ("inferred-type-boolean", "Boolean"),
        ("inferred-type-date", "Date"),
        ("inferred-type-time", "Time"),
        ("inferred-type-datetime", "DateTime"),
        ("inferred-type-string", "String")
    )

    keys.map {
      case (key, dataType) =>
        if(maxType.equalsIgnoreCase(dataType)) {(key, true)}
        else {(key, false)}
    }.toMap
  }
}

case class UniqueValuesExtractor() extends AttributePreprocessor {
  override def preprocess(attribute: Attribute): Map[String,Any] = {
    val values = attribute.values.filter(_.trim.nonEmpty)
    val counts: Map[String,Int] = values
      .groupBy(identity)
      .map { case (k,v) => (k,v.size) }

    val total = counts.values.sum.toDouble
    // val avgInstances = total.toDouble / counts.size.toDouble
    // val isDiscrete = (avgInstances >= 0.05 * total)
    val isDiscrete: Boolean = (counts.size.toDouble / total) <= 0.3 //number of unique values is lte 30% of rowcount
    val entropy: Double =  if (isDiscrete) {
        counts.map {
          case (k, v) =>
            val prob = v.toDouble / total
            prob * Math.log(prob)
        }.sum * -1
    } else {
      -1.0
    }
    Map("histogram-of-content-data" -> counts,
      "is-discrete" -> isDiscrete,
      "entropy" -> entropy,
      "num-unique-vals" -> counts.size)
  }
}

case class StringLengthStatsExtractor() extends AttributePreprocessor {
  override def preprocess(attribute: Attribute): Map[String,Any] = {
    val lengths = attribute.values.map(_.length)
    val mean: Double = lengths.sum.toDouble / lengths.size.toDouble

    val sortedLengths = lengths.sorted
    val median: Double = if(sortedLengths.size > 1) {
      sortedLengths(Math.ceil(lengths.size.toDouble/2.0).toInt - 1)
    } else {
      -1
    }

    val mode: Double = if(lengths.nonEmpty) {
      lengths.groupBy(identity).maxBy(_._2.size)._1
    } else {
      -1
    }

    val max: Double = lengths.foldLeft(0)({case (mx,v) => {if(v > mx) v else mx}})
    val min: Double = lengths.foldLeft(max)({case (mn,v) => {if(v < mn) v else mn}})
    Map("string-length-stats" -> List(mean,median,mode,min,max))
  }
}