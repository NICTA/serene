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
import com.typesafe.scalalogging.LazyLogging

case class CSVDataLoader(val id: String = "", val encoding: String = "utf-8") extends FileLoaderTrait[DataModel] with LazyLogging {

    def load(path: String): DataModel = {

      logger.info(s"loading file $path")
        //path can point to either a directory or a file
        val loaded = if((new File(path)).isDirectory()) {
            loadDirectory(path)
        } else {
            loadFile(path)
        }
      logger.info("done with loading")
      loaded
    }

    def loadDirectory(path: String): DataModel = {
        val tableNameRegex = """^(.+).csv""".r
        val tableNames = new File(path).list.filter({
            case tableNameRegex(a) => true
            case _ => false
        }).map({
            case tableNameRegex(a) => a + ".csv"
        })

        lazy val csvData: DataModel = new DataModel(id, Some(Metadata("CSV Dataset", "CSV Dataset")), None, Some(tables))
        lazy val tables: List[DataModel] = tableNames.map(loadTable(path,_,id,Some(csvData))).toList

        csvData
    }

    def loadFile(path: String): DataModel = {
        val source = Source.fromFile(path, encoding)
        val lines = source.getLines.toList
        val filename = path.substring(path.lastIndexOf("/")+1, path.length)
        source.close
        parseCsv(lines, filename, "", None)
    }

    def loadTable(path: String, tableName: String, parentId: String): DataModel = {
        loadTable(path, tableName, parentId, None)
    }

    def loadTable(path: String, tableName: String, parentId: String, parent: => Option[DataModel]): DataModel = {
        val source = Source.fromFile(s"$path/$tableName", encoding)
        val lines = source.getLines.toList
        source.close
        parseCsv(lines, tableName, parentId, parent)
    }

    def parseCsv(lines: Seq[String], tableName: String, parentId: String, parent: => Option[DataModel]): DataModel = {
      val quotedRegex = "\"(.*)\"" .r
      val attrHeaders = lines.head.split(""",(?=(?:[^\"]*\"[^\"]*\")*(?![^\"]*\"))""", -1)
        .toList
        .map {
          case quotedRegex(a) => a.trim()
          case x => x.trim()
        }

      //we set metadata to be empty if headers are all numbers from 0 to #headers
      //this is the assumption that these headers are just subsitute for None
      val procNames = if (attrHeaders.forall(_.forall(Character.isDigit))) {
        if (attrHeaders.map(_.toInt).forall( _ <= attrHeaders.size)) {
          List.fill(attrHeaders.size)(None)
        } else { attrHeaders.map( x => Some(Metadata(x,""))) }
      } else { attrHeaders.map( x => Some(Metadata(x,""))) }
//      val procNames =attrHeaders.map( x => Some(Metadata(x,"")))

      val attrIds = if(parentId.nonEmpty) {
        attrHeaders.map(attr => s"$attr@$tableName@$parentId")
      } else {
        attrHeaders.map(attr => s"$attr@$tableName")
      }

      val attrVals = lines.drop(1)
        .map { line => line.split(""",(?=(?:[^\"]*\"[^\"]*\")*(?![^\"]*\"))""", -1).toList}
        .filter { line => !line.forall(_.length == 0)} // we filter out empty strings

      lazy val table: DataModel = new DataModel(tableName, Some(Metadata(tableName,"")), parent, Some(attributes))
      lazy val attributes: List[Attribute] = attrHeaders.indices.map {
        idx =>
          new Attribute(attrIds(idx),
            procNames(idx),
            attrVals.map {
              case tokens if tokens.size > idx =>
                val t = tokens(idx)
                if(t.length > 1 && t.startsWith("\"") && t.endsWith("\"")) t.substring(1,t.length-1) //remove quotes
                else t
              case _ => ""
          }.toList,
          Some(table))
      }.toList

      table
    }
}