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

import au.csiro.data61.ingest.JsonSchema.{OptionalFieldKey, SchemaFieldKey, TypeFieldKey}
import au.csiro.data61.ingest.JsonType.{JsonType, PrimitiveJsonTypes}
import org.json4s._

case class JsonSchema(
    props: Map[String, JsonType.ValueSet],
    subSchemas: Map[String, JsonSchema],
    subPrimitiveArrayTypes: Map[String, JsonType.ValueSet],
    optionalProps: Set[String]) {

  def toJsonAst: JObject = new JObject(props
    .map { prop =>
      val (key, value) = prop

      def toJsonArray(types: JsonType.ValueSet): JArray =
        JArray(value.toList.sorted.map(_.toString).map(JString(_)))

      val typeField = if (value.size == 1) {
        JField(TypeFieldKey, JString(value.head.toString))
      } else {
        JField(TypeFieldKey, toJsonArray(value))
      }
      val fields = List(typeField)

      val schemaField =
        if (subPrimitiveArrayTypes contains key) {
          val types = subPrimitiveArrayTypes(key)
          if (types.size == 1) {
            Some(JField(SchemaFieldKey, JString(types.mkString("|"))))
          } else {
            Some(JField(SchemaFieldKey, toJsonArray(types)))
          }
        } else if (subSchemas contains key) {
          Some(JField(SchemaFieldKey, subSchemas(key).toJsonAst))
        } else {
          None
        }

      val optionalField =
        if (optionalProps contains key) {
          Some(JField(OptionalFieldKey, JBool(true)))
        } else {
          None
        }

      (key, new JObject(fields ++ schemaField ++ optionalField))
    }
    .toList
  )
}

object JsonSchema {
  val VirtualRootKey = "$$$VIRTUAL_ROOT$$$"
  val TypeFieldKey = "type"
  val OptionalFieldKey = "optional"
  val SchemaFieldKey = "schema"

  def empty(): JsonSchema = JsonSchema(
    props = Map.empty,
    subSchemas = Map.empty,
    subPrimitiveArrayTypes = Map.empty,
    optionalProps = Set.empty
  )

  def from(jsonValue: JValue): JsonSchema = jsonValue match {
    case JObject(obj) => from(obj)
    case _ => from(Seq(JField(VirtualRootKey, jsonValue)))
  }

  def from(fields: Seq[JField]): JsonSchema = {
    val props = fields
      .map(f => (f._1, JsonType.ValueSet(jsonType(f._2))))
      .toMap

    val subObjectSchemas = fields
      .filter {
        case (_, JObject(_)) => true
        case _ => false
      }
      .map(field => (field._1, from(field._2.asInstanceOf[JObject])))

    val subObjectArraySchemas = fields
      .filter {
        case (_, value @ JArray(_)) if containsOnlyObjectElements(value) => true
        case _ => false
      }
      .map { field =>
        val elemSchemas = field._2.asInstanceOf[JArray].arr.map {
          elem => from(elem.asInstanceOf[JObject])
        }
        (field._1, merge(elemSchemas))
      }

    val subPrimitiveArrayTypes = fields
      .filter {
        case (_, value @ JArray(_)) if containsOnlyPrimitiveElements(value) => true
        case _ => false
      }
      .map(field => (
        field._1,
        JsonType.ValueSet(jsonType(field._2.asInstanceOf[JArray].arr.head))
      ))
      .toMap

    JsonSchema(
      props = props,
      subSchemas = (subObjectSchemas ++ subObjectArraySchemas).toMap,
      subPrimitiveArrayTypes = subPrimitiveArrayTypes,
      optionalProps = Set.empty
    )
  }

  protected def containsOnlyPrimitiveElements(jsonArray: JArray): Boolean = jsonArray.arr match {
    case Nil => false
    case elems => elems.forall(elem => PrimitiveJsonTypes.contains(jsonType(elem)))
  }

  protected def containsOnlyObjectElements(jsonArray: JArray): Boolean = jsonArray match {
    case JArray(Nil) => false
    case JArray(arr) =>
      val (objectElems, otherElems) = arr.partition(jsonType(_) == JsonType.Object)
      otherElems.isEmpty || (objectElems.nonEmpty && otherElems.forall(_ == JsonType.Array))
  }

  protected def jsonType(jsonValue: JValue): JsonType = jsonValue match {
    case JInt(_) | JLong(_) | JDouble(_) | JDecimal(_) => JsonType.Number
    case JBool(_) => JsonType.Boolean
    case JString(_) => JsonType.String
    case JObject(_) => JsonType.Object
    case JArray(_) => JsonType.Array
    case JNothing | JNull => JsonType.Null
  }

  def merge(schema1: JsonSchema, schema2: JsonSchema): JsonSchema = {
    def m[A](
        xs: Map[String, A],
        ys: Map[String, A],
        reduceOp: (A, A) => A,
        skip: Set[String] = Set.empty): Map[String, A] =
      (xs.toSeq ++ ys.toSeq)
        .filterNot { x => skip.contains(x._1) }
        .groupBy(_._1)
        .map { x => (x._1, x._2.map(_._2).reduce(reduceOp)) }

    def jsonTypesUnion(ts1: JsonType.ValueSet, ts2: JsonType.ValueSet): JsonType.ValueSet =
      ts1.union(ts2)

    val props =
      m(schema1.props, schema2.props, jsonTypesUnion)

    val heterogeneousPropKeys = props
      .filter { prop =>
        val (key, value) = prop
        value.size > 1 ||
          (schema1.subPrimitiveArrayTypes.contains(key) && schema2.subSchemas.contains(key)) ||
          (schema2.subPrimitiveArrayTypes.contains(key) && schema1.subSchemas.contains(key))
      }
      .keySet

    val subSchemas =
      m(schema1.subSchemas, schema2.subSchemas, merge, heterogeneousPropKeys)

    val subPrimitiveArrayTypes = m(
      schema1.subPrimitiveArrayTypes,
      schema2.subPrimitiveArrayTypes,
      jsonTypesUnion,
      heterogeneousPropKeys
    )

    val optionalProps =
      schema1.optionalProps |
      schema2.optionalProps |
      (schema1.props.keySet &~ schema2.props.keySet) |
      (schema2.props.keySet &~ schema1.props.keySet)

    JsonSchema(
      props = props,
      subSchemas = subSchemas,
      subPrimitiveArrayTypes = subPrimitiveArrayTypes,
      optionalProps = optionalProps
    )
  }

  def merge(schemas: Seq[JsonSchema]): JsonSchema = schemas.reduce(merge)
}