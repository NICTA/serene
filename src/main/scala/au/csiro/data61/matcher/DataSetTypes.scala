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
package au.csiro.data61.matcher

import java.nio.file.{Paths, Path}

import ColumnTypes._
import DataSetTypes._
import org.joda.time.DateTime

import org.json4s._
import org.json4s.jackson.Serialization

/**
 * Types used for the DataSet objects
 */
object DataSetTypes {

  type TypeMap = Map[String, String]

  type DataSetID = Int
}

/**
 * Types used for the column objects
 */
object ColumnTypes {

  type ColumnID = Int
}

/**
 * Testing...
 *
 * @param greeting Testing parameter
 * @param to Testing parameter
 */
case class Message(greeting: String, to: String)


/**
 * LogicalType Enumeration used for the Column types
 */
sealed trait LogicalType { def str: String }

object LogicalType {

  case object STRING  extends LogicalType { val str = "string" }
  case object INTEGER extends LogicalType { val str = "integer" }
  case object FLOAT   extends LogicalType { val str = "float" }
  case object BOOLEAN extends LogicalType { val str = "boolean" }
  case object FACTOR  extends LogicalType { val str = "factor" }

  val values = List(
    STRING,
    INTEGER,
    FLOAT,
    BOOLEAN,
    FACTOR
  )

  def lookup(str: String): Option[LogicalType] = {
    values.find(_.str == str)
  }

}


/**
 * Serializer for the LogicalType
 */
class LogicalTypeSerializer extends CustomSerializer[LogicalType](format => (
  {
    case jv: JValue =>
      implicit val formats = DefaultFormats
      val str = jv.extract[String]
      val logicType = LogicalType.lookup(str)
      logicType getOrElse (throw new Exception("Failed to parse LogicalType"))
  }, {
    case logicalType: LogicalType =>
      JString(logicalType.str)
  }))


/**
 * Column values used by the data set storage layer
 *
 * @param index Column position in the file
 * @param path Original resource location
 * @param name Name of the column
 * @param id Column identifier key
 * @param size The size of the full column array
 * @param datasetID Dataset identifier key
 * @param sample Small sample of the dataset
 * @tparam T Type of the sample dataset
 */
case class Column[+T](index: Int,
                      path: Path,
                      name: String,
                      id: ColumnID,
                      size: Long,
                      datasetID: DataSetID,
                      sample: List[T],
                      logicalType: LogicalType)


/**
 * Dataset object created internally by the data set storage layer
 *
 * @param id Data set id key
 * @param columns Set of column objects
 * @param filename Original filename string
 * @param path Path to the stored resource. Full path.
 * @param typeMap The user specified type mapper
 * @param description The user specified metadata description
 * @param dateCreated The date that the dataset was added
 * @param dateModified The timestamp when the dataset was modified
 */
case class DataSet(id: Int,
                   columns: List[Column[Any]],
                   filename: String,
                   path: Path,
                   typeMap: TypeMap,
                   description: String,
                   dateCreated: DateTime,
                   dateModified: DateTime)


/**
 * Serializer for the Java.io.Path object
 */
class PathSerializer extends CustomSerializer[Path](format => ( {
    case jv: JValue =>
      implicit val formats = DefaultFormats
      val str = jv.extract[String]
      Paths.get(str)
  }, {
    case path: Path =>
      JString(path.toString)
  }))


/**
 * Holds the implicit matcher objects for the Json4s Serializers.
 *
 * This should be mixed in to the object in order to use.
 */
trait MatcherJsonFormats {

  implicit def json4sFormats: Formats =
    org.json4s.DefaultFormats ++
    org.json4s.ext.JodaTimeSerializers.all +
    new LogicalTypeSerializer +
    new PathSerializer

}