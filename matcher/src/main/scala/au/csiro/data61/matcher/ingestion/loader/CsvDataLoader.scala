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
package au.csiro.data61.matcher.ingestion.loader

import java.io._
import java.nio.file.Paths

import au.csiro.data61.matcher.data.Metadata
import au.csiro.data61.matcher.data.DataModel
import au.csiro.data61.matcher.data.Attribute

import org.apache.commons.csv._
import scala.collection.JavaConverters._

import com.typesafe.scalalogging.LazyLogging

import scala.util.Try
import language.postfixOps


case class CsvDataLoader(id: String = "",
                         encoding: String = "utf-8",
                         headerLines: Int = 1) extends FileLoaderTrait[DataModel] with LazyLogging {

  def toIntOpt(x: String): Option[Int] = Try (x.toInt) toOption

  def load(path: String): DataModel = {

    //path can point to either a directory or a file
    val loaded = if(Paths.get(path).toFile.isDirectory) {
      logger.debug(s"loading directory: $path")
      loadDirectory(path)
    } else {
      logger.debug(s"loading file: $path")
      loadTable(path)
    }

    loaded
  }

  def loadDirectory(path: String): DataModel = {

    val tableNames = Paths.get(path).toFile.list
      .filter(_.endsWith(".csv")).map(Paths.get(path, _).toString)

    lazy val csvData: DataModel = new DataModel(id, Some(Metadata("CSV Dataset", "CSV Dataset")), None, Some(tables))
    lazy val tables: List[DataModel] = tableNames.map(loadTable(_, id)).toList

    csvData
  }

  def loadTable(path: String, parentId: String =""): DataModel = {

    val tableName = path.substring(path.lastIndexOf("/") + 1, path.length)

    // first load a CSV object...
    val csv = CSVFormat.RFC4180
      .parse(new FileReader(path))

    // pull into a row list of List[String]
    val rows = csv
      .iterator
      .asScala
      .map { row => (0 until row.size()).map(row.get) }
      .filter { line => !line.forall(_.length == 0)} // we filter out rows which contain empty vals.toList.transpose
      .toList

    // transpose the rows...
    val columns = rows.transpose

    val headers = columns.map(_.take(headerLines).mkString("_"))

    val attrVals = columns.map(_.drop(headerLines))

    //we set metadata to be empty if headers are all numbers from 0 to #headers
    //this is the assumption that these headers are just substitute for None
    val procNames = if (headers.flatMap(toIntOpt).sorted != headers.indices.toList) {
      headers.map(x => Some(Metadata(x, "")))
    } else {
      headers.map(_ => None)
    }

    val attrIds = if (parentId.nonEmpty) {
      headers.map(attr => s"$attr@$tableName@$parentId")
    } else {
      headers.map(attr => s"$attr@$tableName")
    }

    lazy val table: DataModel = new DataModel(tableName, Some(Metadata(tableName,"")), None, Some(attributes))

    lazy val attributes = headers.indices.map {
      idx => new Attribute(attrIds(idx), procNames(idx), attrVals(idx), Some(table))
    }.toList

    table
  }
}