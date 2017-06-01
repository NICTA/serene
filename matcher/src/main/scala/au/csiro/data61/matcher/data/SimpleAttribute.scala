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
package au.csiro.data61.matcher.data

import au.csiro.data61.matcher.nlptools.tokenizer._
import au.csiro.data61.matcher.nlptools.parser._

import scala.util.Random

/**
  * Class to encapsulate attribute with row values.
  * Spark has problems with List.
  * @param attributeName id of the attribute from Attribute class
  * @param metaName name of the attribute from Metadata
  * @param values Array of string values from rows
  */
case class SimpleAttribute(attributeName: String,
                           metaName: Option[String],
                           values: Array[String]
                          ) extends Serializable {

  lazy val attributeNameTokenized: List[String] = getTokens()
  lazy val charDist: Map[Char, Double] = getCharDist()
  lazy val inferredMap: Map[String, Boolean] = getInferredDataMap()

  /**
    * Tokenize attribute name
    * @return
    */
  protected def getTokens(): List[String] = {
    val nameRegex = "([^@]+)@(.+)".r
    metaName match {
      case Some(name: String) => name match {
        case nameRegex(name, _) => StringTokenizer.tokenize(name)
        case x => StringTokenizer.tokenize(x)
      }
      case _ => List()
    }
  }

  /**
    * Calculate normalized character distribution
    * @return
    */
  protected def getCharDist(): Map[Char, Double] = {
    val counts: Map[Char,Int] = values
      .flatMap(_.toCharArray)
      .groupBy(identity)
      .mapValues(_.length)

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
        case 0 => counts.map { case (x, _) => (x, 0.0) }
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

      normCf
    } else {
      Map()
    }
  }

  /**
    * Calculate map of inferred data types
    * @return
    */
  protected def getInferredDataMap(): Map[String, Boolean] = {
    val maxSample = 100

    val samples = new Random(18371623)
      .shuffle(values.toList.filter(_.nonEmpty))
      .take(maxSample)
    val maxType = if(samples.nonEmpty) {
      val infDataTypes = DataTypeParser.inferDataTypes(samples.toList)
      val typeCount = infDataTypes.foldLeft(Map[String,Int]())(
        (counts, dtype) => counts + (dtype -> (
          counts.getOrElse(dtype, 0) + 1)))
      typeCount.toList.maxBy(_._2)._1
    } else {
      "String" //default to string
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
        (key, maxType.equalsIgnoreCase(dataType))
    }.toMap
  }
}

//
//preprocesseddatamap
//Map("attribute-name-tokenized" -> List[String])
//Map("normalised-char-frequency-vector" -> Map[Char, Double])
//("inferred-type-float", "Float")
//("inferred-type-integer", "Integer")
//("inferred-type-long", "Long")
//("inferred-type-boolean", "Boolean")
//("inferred-type-date", "Date")
//("inferred-type-time", "Time")
//("inferred-type-datetime", "DateTime")
//("inferred-type-string", "String") -> bool
//
//Map("histogram-of-content-data" -> Map[String,Int],
//"is-discrete" -> bool,
//"entropy" -> double,
//"num-unique-vals" -> int)

//Map("string-length-stats" -> List(mean,median,mode,min,max))
