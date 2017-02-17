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

import au.csiro.data61.ingest.JsonType.JsonType
import org.json4s._

/**
  * Represents a JSON schema.
  * @param schemaType The JSON type that this schema describes.
  * @param propertySchemas The schemas of properties if this schema represents a JSON object.
  * @param elementSchemas The schemas of elements if this schema represents a JSON array.
  * @param optionalProperties The optional properties if this schema represents a JSON object.
  */
case class JsonSchema(
    schemaType: JsonType,
    propertySchemas: Map[String, Seq[JsonSchema]],
    elementSchemas: Seq[JsonSchema],
    optionalProperties: Set[String])

/**
  * Contains operations for JSON schemas.
  */
object JsonSchema {
  val TypeFieldKey = "type"
  val OptionalFieldKey = "optional"
  val ObjectSchemaFieldKey = "objectSchema"
  val ArraySchemaFieldKey = "elementSchema"

  /**
    * Extracts the schema of a JSON value.
    * @param jsonValue The JSON value.
    * @return The extracted schema.
    */
  def from(jsonValue: JValue): JsonSchema = {
    val schemaType = jsonType(jsonValue)

    val propertySchemas: Map[String, Seq[JsonSchema]] = if (schemaType == JsonType.Object) {
      jsonValue.asInstanceOf[JObject].obj.map(field => (field._1, Seq(from(field._2)))).toMap
    } else {
      Map.empty
    }

    val elementSchemas = if (schemaType == JsonType.Array) {
      jsonValue.asInstanceOf[JArray].arr.map(from).foldLeft(Seq.empty[JsonSchema])(merge)
    } else {
      Seq.empty
    }

    JsonSchema(
      schemaType = schemaType,
      propertySchemas = propertySchemas,
      elementSchemas = elementSchemas,
      optionalProperties = Set.empty
    )
  }

  protected def jsonType(jsonValue: JValue): JsonType = jsonValue match {
    case JInt(_) | JLong(_) | JDouble(_) | JDecimal(_) => JsonType.Number
    case JBool(_) => JsonType.Boolean
    case JString(_) => JsonType.String
    case JObject(_) => JsonType.Object
    case JArray(_) => JsonType.Array
    case JNothing | JNull => JsonType.Null
  }

  def merge(schema1: JsonSchema, schema2: JsonSchema): Seq[JsonSchema] =
    merge(Seq(schema1), schema2)

  /**
    * Merges a JSON schema into a collection of JSON schemas.
    *
    * Denoting the collection as S and the schema to be merged into S as x, the rules for merging
    * are as follows:
    * - If there is a schema y in S that has the same type as x, then:
    *   - If the type is boolean, number, string or null, discard x and return S.
    *   - If the type is object, merge x's property schemas into y's and update y's optional
    *     properties with x's.
    *   - If the type is array, merge x's element schemas into y's.
    * - Otherwise, append x to S.
    * @param schemas The collection of schemas.
    * @param schema The schema to be merged into the collection.
    * @return The merged collection of schemas.
    */
  def merge(schemas: Seq[JsonSchema], schema: JsonSchema): Seq[JsonSchema] = {
    def m(origin: JsonSchema): JsonSchema = origin match {
      case JsonSchema(JsonType.Object, propertySchemas, _, optionalProperties) =>
        val mergedPropertySchemas = (propertySchemas.toSeq ++ schema.propertySchemas.toSeq)
          .groupBy(_._1)
          .map(x => (x._1, x._2.map(_._2)))
          .map {
            case (key, Seq(ss1, ss2)) => (key, ss1.foldLeft(ss2)(merge))
            case (key, value) => (key, value.flatten)
          }

        origin.copy(
          propertySchemas = mergedPropertySchemas,
          optionalProperties = optionalProperties | schema.optionalProperties | (
            mergedPropertySchemas.keySet &~ (
              propertySchemas.keySet & schema.propertySchemas.keySet
            )
          )
        )
      case JsonSchema(JsonType.Array, _, elementSchemas, _) =>
        origin.copy(
          elementSchemas = elementSchemas.foldLeft(schema.elementSchemas)(merge)
        )
      case x => x
    }


    if (schemas.exists(_.schemaType == schema.schemaType)) {
      schemas collect {
        case origin if origin.schemaType == schema.schemaType => m(origin)
        case x => x
      }
    } else {
      schemas :+ schema
    }
  }

  def toJsonAst(schema: JsonSchema): JValue = toJsonAst(Seq(schema), false)

  /**
    * Converts a collection of JSON schemas to a JSON abstract syntax tree.
    * @param schemas The collection of schemas.
    * @param optional Whether the schemas are optional. This is meaningful only in recursive calls
    *                 so defaults to false.
    * @return The JSON abstract syntax tree representing the schemas.
    */
  def toJsonAst(schemas: Seq[JsonSchema], optional: Boolean = false): JValue = schemas match {
    case Seq(JsonSchema(schemaType, propertySchemas, elementSchemas, optionalProperties)) =>
      val typeField = JField(TypeFieldKey, JString(schemaType.toString))

      val optionalField = if (optional) {
        Some(JField(OptionalFieldKey, JBool(optional)))
      } else {
        None
      }

      val objectSchemaField = if (schemaType == JsonType.Object) {
        val fields = propertySchemas map {
          ps => JField(ps._1, toJsonAst(ps._2, optionalProperties.contains(ps._1)))
        }
        Some(JField(
          ObjectSchemaFieldKey,
          JObject(fields.toList)
        ))
      } else {
        None
      }

      val arraySchemaField = if (schemaType == JsonType.Array) {
        Some(JField(ArraySchemaFieldKey, toJsonAst(elementSchemas)))
      } else {
        None
      }

      JObject(List(typeField) ++ optionalField ++ objectSchemaField ++ arraySchemaField)
    case _ => JArray(schemas.map(s => toJsonAst(Seq(s))).toList)
  }
}