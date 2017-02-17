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
package au.csiro.data61.ingest

import org.json4s.native.JsonMethods.{compact, render}
import org.json4s.{JArray, JField, JNothing, JNull, JObject, JValue}

import scala.annotation.tailrec

/**
  * Contains transformation methods for JSON values.
  */
object JsonTransforms {
  val UnknownHeader = "$$$Unknown$$$"

  /**
    * Tests if a JSON value is flat.
    *
    * The following JSON values are considered flat:
    * - Boolean, number, string and null values
    * - Object values without array or object properties
    * @param jsonValue The JSON value.
    * @return true if the JSON value is flat.
    */
  def flat(jsonValue: JValue): Boolean = jsonValue match {
    case jsonObject @ JObject(_) => !(jsonObject.obj exists {
      case (_, JArray(_)) => true
      case (_, JObject(_)) => true
      case _ => false
    })
    case JArray(_) => false
    case _ => true
  }

  /**
    * Flattens a JSON value iteratively until all results are flat JSON values.
    * @param jsonValue The JSON value.
    * @return The collection of flat JSON values.
    */
  def flattenMax(jsonValue: JValue): Seq[JValue] = flattenMax(flatten(jsonValue))

  def flattenMax(jsonValues: Seq[JValue]): Seq[JValue] = {
    @tailrec
    def f(xs: Seq[JValue], ys: Seq[JValue]): Seq[JValue] = xs match {
      case Nil => ys
      case _ =>
        val partition = xs.flatMap(flatten).partition(flat)
        f(partition._2, partition._1 ++ ys)
    }

    val partition = jsonValues.partition(flat)
    f(partition._2, partition._1)
  }

  /**
    * Flattens a JSON value so that its depth is reduced at most by 1.
    * @param jsonValue The JSON value.
    * @return The collection of flattened JSON values.
    */
  def flatten(jsonValue: JValue): Seq[JValue] = jsonValue match {
    case jsonObject @ JObject(_) => flattenObject(jsonObject)
    case JArray(values) => values
    case value => Seq(value)
  }

  def flattenObject(jsonObject: JObject): Seq[JObject] = {
    if (flat(jsonObject)) {
      Seq(jsonObject)
    } else {
      resolveDuplicateProps(jsonObject) flatMap { jsonObj =>
        jsonObj.obj.foldLeft(Seq(jsonObj)) {
          (results, field) =>
            results flatMap {
              result =>
                field match {
                  case (key, value @ JArray(_)) => flattenArrayProp(result, key, value)
                  case (key, JObject(_)) => Seq(flattenObjectProp(result, key))
                  case _ => Seq(result)
                }
            }
        }
      }
    }
  }

  protected def resolveDuplicateProps(jsonObject: JObject): Seq[JObject] = {
    val fieldGroups = jsonObject.obj.groupBy(_._1).toSeq
    val duplicateFields = fieldGroups filter (_._2.size > 1) map {
      field => (field._1, field._2.map(_._2))
    }

    duplicateFields match {
      case Nil => Seq(jsonObject)
      case _ =>
        val fs = duplicateFields map {
          field => (xs: Seq[JObject]) => xs flatMap {
            x => flattenDuplicateProp(x, field._1, field._2)
          }
        }
        Function.chain(fs).apply(Seq(jsonObject))
    }
  }

  protected def flattenDuplicateProp(
      jsonObject: JObject, propKey: String, propValues: Seq[JValue]): Seq[JObject] =
    propValues map { propValue =>
      val otherFields = jsonObject.obj filterNot {_._1 == propKey}
      JObject(otherFields :+ (propKey, propValue))
    }

  protected def flattenArrayProp(
      jsonObject: JObject, propKey: String, propValue: JArray): Seq[JObject] = {
    val otherFields = jsonObject.obj filterNot {_._1 == propKey}

    if (propValue.arr.nonEmpty) {
      propValue.arr map {
        elem => JObject(otherFields :+ (propKey, elem))
      }
    } else {
      Seq(JObject(otherFields))
    }
  }

  protected def flattenObjectProp(jsonObject: JObject, propKey: String): JObject =
    JObject(jsonObject.obj flatMap {
      case (key, value @ JObject(_)) if key == propKey =>
        value.obj.map { case (k, v) => (s"$key.$k", v) }
      case (key, value) => Seq((key, value))
    })

  /**
    * Converts a collection of JSON values to CSV lines.
    *
    * This method will convert only JSON objects to CSV lines. Other types of JSON values are
    * filtered out and returned as part of the result.
    * @param jsonValues The JSON values.
    * @return A tuple with 3 elements. The first element is the headers of the CSV lines, extracted
    *         from the keys of the JSON objects. The second element is the CSV lines. The third
    *         element is the non-object JSON values filtered out.
    */
  def toCsv(jsonValues: Seq[JValue]): (Seq[String], Seq[Seq[String]], Seq[JValue]) = {
    val jsonObjects = jsonValues collect { case x @ JObject(_) => x }

    val keys = jsonObjects.flatMap(_.obj.map(_._1)).toSet

    val lines = jsonObjects map { jsonObject =>
      val flatJsonObjectKeys = jsonObject.obj.map(_._1).toSet
      val fields = appendNullFields(jsonObject.obj, keys &~ flatJsonObjectKeys).sortBy(_._1)
      toCompactValues(fields)
    }

    (keys.toSeq.sorted, lines, jsonValues.filterNot(_.isInstanceOf[JObject]))
  }

  protected def appendNullFields(fields: Seq[JField], keysOfNull: Set[String]): Seq[JField] =
    fields ++ (keysOfNull map {key => JField(key, JNull)})

  protected def toCompactValues(fields: Seq[JField]): Seq[String] = fields.map {
    case (_, value @ JObject(_)) => compact(render(value))
    case (_, value @ JArray(_)) => compact(render(value))
    case (_, JNothing | JNull) => ""
    case (_, value) => value.values.toString
  }
}
