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

import java.io.File

import au.csiro.data61.ingest.JsonTransforms.{flattenMax, toCsv}
import com.github.tototoshi.csv.CSVWriter
import org.json4s.Xml.toJson

import scala.xml.XML.loadFile

/**
  * Contains transforms from XML to other data formats.
  */
object XmlIngestor {
  /**
    * Converts a XML file to a CSV file.
    * @param xmlFile The XML file.
    * @param csvFile The CSV file.
    */
  def convertToCsv(xmlFile: File, csvFile: File): Unit = {
    val json = toJson(loadFile(xmlFile))
    val flatJsonObjects = flattenMax(json)
    val (headers, lines, _) = toCsv(flatJsonObjects)
    val writer = CSVWriter.open(csvFile)
    writer.writeRow(headers)
    writer.writeAll(lines)
    writer.close()
  }
}
