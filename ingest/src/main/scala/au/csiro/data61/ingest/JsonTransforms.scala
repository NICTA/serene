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

import org.json4s._

object JsonTransforms {
  def flatten(jsonObject: JObject): Seq[JObject] = {
    jsonObject.obj.foldLeft(Seq(jsonObject)) {
      (results, field) => results flatMap {
        result => field match {
          case (key, _: JArray) => flattenArrayProp(result, key)
          case (key, _: JObject) => Seq(flattenObjectProp(result, key))
          case _ => Seq(result)
        }
      }
    }
  }

  def flattenArrayProp(jsonObject: JObject, propKey: String): Seq[JObject] = {
    val otherFields = jsonObject.obj filterNot {_._1 == propKey}

    jsonObject.obj find {_._1 == propKey} match {
      case Some((_, jsonArray: JArray)) if jsonArray.arr.nonEmpty =>
        jsonArray.arr map {
          jsonValue => JObject(otherFields :+ (propKey, jsonValue))
        }
      case _ => Seq(JObject(otherFields))
    }
  }

  def flattenObjectProp(jsonObject: JObject, propKey: String): JObject = {
    JObject(jsonObject.obj flatMap {
      case (key, value: JObject) if key == propKey =>
        value.obj.map { case (k, v) => (s"$key.$k", v) }
      case (key, value) => Seq((key, value))
    })
  }
}
