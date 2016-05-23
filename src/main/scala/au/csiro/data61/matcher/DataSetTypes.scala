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

import java.nio.file.Path
import java.util.Date

import ColumnTypes._
import DataSetTypes._


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
object Column {
  def apply(col: ColumnPublic[Any]): Column[Any] = {
    Column(
      name = col.name,
      id = col.id,
      datasetID = col.datasetID,
      sample = col.sample,
      logicalType = LogicalType.lookup(col.logicalType).getOrElse(LogicalType.STRING)
    )
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
                   dateCreated: Date,
                   dateModified: Date)

object DataSet {
  def apply(ds: DataSetPublic): DataSet = {
    DataSet(
      id = ds.id,
      columns = ds.columns.map(Column(_)),
      filename = ds.filename,
      path = null,
      typeMap = ds.typeMap,
      description = ds.description,
      dateCreated = ds.dateCreated,
      dateModified = ds.dateModified
    )
  }
}

/** This is used by the IntegrationAPI above as the public type of Column
 *
 * @param name Column name from the original data set
 * @param id ID of the column itself
 * @param datasetID ID of the original data set
 * @param sample Small sample of the column data
 * @param logicalType The logical type inferred or user specified
 * @tparam T The type of the samples
 */
case class ColumnPublic[+T](name: String,
                            id: ColumnID,
                            datasetID: DataSetID,
                            sample: List[T],
                            logicalType: String)
object ColumnPublic {
  def apply[A](col: Column[A]): ColumnPublic[A] = {
    ColumnPublic(
      col.name,
      col.id,
      col.datasetID,
      col.sample,
      col.logicalType.str)
  }
}

/**
 * Contains the IntegrationAPI version of the DataSet object
 *
 * @param id The dataset id value
 * @param description The metadata description of the dataset
 * @param dateCreated The java.util.Date when the item was created
 * @param dateModified The java.util.Date when the item was last modified
 * @param filename The single filename of the original resource
 * @param columns The object of public column values
 * @param typeMap The type map added by the user
 */
case class DataSetPublic(id: Int,
                         description: String,
                         dateCreated: Date,
                         dateModified: Date,
                         filename: String,
                         columns: List[ColumnPublic[Any]],
                         typeMap: TypeMap)

object DataSetPublic {
  def apply(ds: DataSet): DataSetPublic = {
    DataSetPublic(
      id = ds.id,
      description = ds.description,
      dateCreated = ds.dateCreated,
      dateModified = ds.dateModified,
      filename = ds.filename,
      columns = ds.columns.map(ColumnPublic(_)),
      typeMap = ds.typeMap
    )
  }
}