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
import java.nio.file.Paths

import scala.io.Source

import au.csiro.data61.matcher.data.Metadata
import au.csiro.data61.matcher.data.DataModel
import au.csiro.data61.matcher.data.Attribute

case class CSVHierarchicalDataLoader(val encoding: String = "utf-8") {

    def readDataSets(rootDataSetPath: String, relFilePath: String): List[DataModel] = {
        val absFilePath = Paths.get(rootDataSetPath, relFilePath).toString
        val f = new File(absFilePath)

        if(f.isDirectory) {
            f.listFiles.filter({!_.getName.startsWith(".")}).toList.flatMap({case cf =>
                val childPath = cf.getAbsolutePath
                val pathRegex = s"$rootDataSetPath/(.+)".r
                childPath match {
                    case pathRegex(relPath) => readDataSets(rootDataSetPath, relPath)
                }
            })
        } else {
            List(readDataModel(f, relFilePath))
        }
    }

    def readDataSet(rootDataSetPath: String, relFilePath: String): DataModel = {
        val absFilePath = Paths.get(rootDataSetPath, relFilePath).toString
        readDataModel(new File(absFilePath), relFilePath)
    }

    def readDataModel(file: File, dataSetId: String): DataModel = {
        val source = Source.fromFile(file, encoding)
        val lines = source.getLines.toList
        source.close

        val quotedRegex = "\"(.*)\"" .r
        val attrHeaders = lines.head.split(""",(?=(?:[^\"]*\"[^\"]*\")*(?![^\"]*\"))""", -1).toList.map({
            case quotedRegex(a) => a.trim()
            case x => x.trim()
        })
        val attrVals = lines.drop(1).map({case line => line.split(""",(?=(?:[^\"]*\"[^\"]*\")*(?![^\"]*\"))""", -1).toList})

        lazy val table: DataModel = new DataModel(dataSetId, Some(Metadata(dataSetId,"")), None, Some(attributes))
        lazy val attributes: List[Attribute] = (0 until attrHeaders.size).map({case idx => {
                new Attribute(s"$dataSetId/${attrHeaders(idx)}",
                              Some(Metadata(attrHeaders(idx),"")),
                              attrVals.map({
                                case tokens if tokens.size > idx =>
                                    val t = tokens(idx)
                                    if(t.length > 1 && t.startsWith("\"") && t.endsWith("\"")) t.substring(1,t.length-1) //remove quotes
                                    else t
                                case _ => ""
                              }).toList,
                              Some(table)
                              )
            }
        }).toList

        table
    }
}
