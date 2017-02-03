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
import org.json4s.{JArray, JField, JNull, JObject, JNothing, JValue}

object JsonTransforms {
  def flatten(jsonObjects: Seq[JObject]): Seq[JObject] = jsonObjects.flatMap(flatten)

  def flatten(jsonObject: JObject): Seq[JObject] = {
    resolveDuplicateProps(jsonObject) flatMap { jsonObj =>
      jsonObj.obj.foldLeft(Seq(jsonObj)) {
        (results, field) =>
          results flatMap {
            result =>
              field match {
                case (key, value: JArray) => flattenArrayProp(result, key, value)
                case (key, _: JObject) => Seq(flattenObjectProp(result, key))
                case _ => Seq(result)
              }
          }
      }
    }
  }

  def resolveDuplicateProps(jsonObject: JObject): Seq[JObject] = {
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

  def flattenDuplicateProp(
      jsonObject: JObject, propKey: String, propValues: Seq[JValue]): Seq[JObject] =
    propValues map { propValue =>
      val otherFields = jsonObject.obj filterNot {_._1 == propKey}
      JObject(otherFields :+ (propKey, propValue))
    }

  def flattenArrayProp(
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

  def flattenObjectProp(jsonObject: JObject, propKey: String): JObject =
    JObject(jsonObject.obj flatMap {
      case (key, value: JObject) if key == propKey =>
        value.obj.map { case (k, v) => (s"$key.$k", v) }
      case (key, value) => Seq((key, value))
    })

  def appendNullFields(fields: Seq[JField], keysOfNull: Set[String]): Seq[JField] =
    fields ++ (keysOfNull map {key => JField(key, JNull)})

  def sortFieldsByKeys(fields: Seq[JField]): Seq[JField] = fields.sortBy(_._1)

  def toCompactFields(fields: Seq[JField]): Seq[(String, String)] = fields.map {
    case (key, value: JObject) => (key, compact(render(value)))
    case (key, value: JArray) => (key, compact(render(value)))
    case (key, JNothing) => (key, "")
    case (key, JNull) => (key, "")
    case (key, value) => (key, value.values.toString)
  }
}
