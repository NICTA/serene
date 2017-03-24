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

import java.io.File

import scala.io.Source

import au.csiro.data61.matcher.data.Metadata
import au.csiro.data61.matcher.data.DataModel
import au.csiro.data61.matcher.data.Attribute
import au.csiro.data61.matcher.data._
import com.github.tototoshi.csv._

import scala.util.Try
import language.postfixOps


case class CsvDataLoader(val id: String = "",
                         val encoding: String = "utf-8",
                         val headerLines: Int = 1) extends FileLoaderTrait[DataModel] {

  def toIntOpt(x: String): Option[Int] = Try (x.toInt) toOption

  def load(path: String): DataModel = {

    val tableName = path.substring(path.lastIndexOf("/") + 1, path.length)

    val columns = CSVReader.open(new File(path)).all().transpose

    val headers = columns.map(_.take(headerLines).mkString("_"))

    val data = columns.map(_.drop(headerLines))

    //we set metadata to be empty if headers are all numbers from 0 to #headers
    //this is the assumption that these headers are just subsitute for None
    val procNames = if (headers.flatMap(toIntOpt).sorted != headers.indices.toList) {
      headers.map(x => Some(Metadata(x, "")))
    } else {
      headers.map(_ => None)
    }

    val attrIds = headers.map(attr => s"$attr@$tableName")

    val attrVals = data.map(col => col.filter(_.length > 0))

    lazy val table: DataModel = new DataModel(tableName, Some(Metadata(tableName,"")), None, Some(attributes))

    lazy val attributes = headers.indices.map {
      idx => new Attribute(attrIds(idx), procNames(idx), attrVals(idx), Some(table))
    }.toList

    table
  }
}