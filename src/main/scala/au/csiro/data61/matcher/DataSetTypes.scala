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
import com.typesafe.scalalogging.LazyLogging
import org.joda.time.DateTime

import play.api.libs.json._

import scala.util.{Failure, Success, Try}


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

  implicit val jsonWrites = new Writes[LogicalType] {
    def writes(s: LogicalType): JsValue = JsString(s.str)
  }

  implicit val jsonReads = new Reads[LogicalType] {
    def reads(json: JsValue): JsResult[LogicalType] = {
      lookup(json.as[String]) match {
        case Some(lt) =>
          JsSuccess(lt)
        case _ =>
          JsError(s"Failed to parse: $json")
      }
    }
  }
}


/**
 * Column values used by the data set storage layer
 *
 * @param name Name of the column
 * @param id Column identifier key
 * @param datasetID Dataset identifier key
 * @param sample Small sample of the dataset
 * @tparam T Type of the sample dataset
 */
case class Column[+T](name: String,
                      id: ColumnID,
                      datasetID: DataSetID,
                      sample: List[T],
                      logicalType: LogicalType)

object Column extends LazyLogging {

  implicit val jsonWrites = new Writes[Column[Any]] {
    def writes(col: Column[Any]): JsValue = JsObject(Seq(
      "name" -> JsString(col.name),
      "id" -> JsNumber(col.id),
      "datasetID" -> JsNumber(col.id),
      "sample" -> JsArray(col.sample.map {
        case x: Int => JsNumber(x)
        case x: String => JsString(x)
        case x: Double => JsNumber(x)
        case x: Boolean => JsBoolean(x)
        case x => JsString(x.toString)
      }),
      "logicalType" -> Json.toJson(col.logicalType)
    ))
  }

  implicit val jsonReads = new Reads[Column[Any]] {
    def reads(json: JsValue): JsResult[Column[Any]] = {
      Try {
        Column(
          name = (json \ "name").as[String],
          id = (json \ "id").as[ColumnID],
          datasetID = (json \ "datasetID").as[DataSetID],
          sample = (json \ "sample").as[List[JsValue]] match {
            case req @ List(JsString(x), _*) =>
              req.map(_.as[String])
            case req @ List(JsNumber(x), _*) =>
              req.map(_.as[Double])
            case req @ List(JsBoolean(x), _*) =>
              req.map(_.as[Boolean])
            case req =>
              logger.warn(s"Couldn't determine type of column: $req")
              req.map(_.as[String])
          },
          logicalType = (json \ "logicalType").as[LogicalType]
        )
      }
    } match {
        case Success(col) =>
          JsSuccess(col)
        case Failure(_) =>
          JsError("Failed to parse column")
    }
  }

}

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
object DataSet {

  val pattern = "yyyy-MM-dd'T'HH:mm:ssz"

  implicit val dateFormat =
    Format[DateTime](Reads.jodaDateReads(pattern), Writes.jodaDateWrites(pattern))


  implicit val jsonReads = new Reads[DataSet] {
    def reads(json: JsValue): JsResult[DataSet] = {
      Try {
        DataSet(
          id = (json \ "id").as[Int],
          columns = (json \ "columns").as[List[Column[Any]]],
          filename = (json \ "filename").as[String],
          path = Paths.get((json \ "path").as[String]),
          typeMap = (json \ "typeMap").as[TypeMap],
          description = (json \ "description").as[String],
          dateCreated = (json \ "dateCreated").as[DateTime],
          dateModified = (json \ "dateModified").as[DateTime]
        )
      }
    } match {
      case Success(col) =>
        JsSuccess(col)
      case Failure(_) =>
        JsError("Failed to parse dataset")
    }
  }

  implicit val jsonWrites = new Writes[DataSet] {
    def writes(ds: DataSet): JsValue = JsObject(Seq(
      "id" -> JsNumber(ds.id),
      "columns" -> JsArray(ds.columns.map(Json.toJson(_))),
      "filename" -> JsString(ds.filename),
      "path" -> JsString(ds.path.toString),
      "typeMap" -> JsObject(ds.typeMap.mapValues(JsString)),
      "description" -> JsString(ds.description),
      "dateCreated" -> Json.toJson(ds.dateCreated),
      "dateModified" -> Json.toJson(ds.dateModified)
    ))
  }
}
