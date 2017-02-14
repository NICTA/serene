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

import java.io.{File, PrintWriter}
import java.nio.file.Files

import au.csiro.data61.ingest.JsonTransforms.{flattenMax, toCsv}
import com.github.tototoshi.csv.CSVWriter
import org.json4s.native.JsonMethods.{parse, pretty, render}

import scala.collection.JavaConversions.asScalaBuffer
import scala.collection.JavaConverters.asScalaIteratorConverter

object JsonIngestor {
  def convertJsonToCsv(jsonFile: File, csvFile: File): Unit = {
    val flatJsonObjects = flattenMax(parse(jsonFile))
    val (headers, lines, _) = toCsv(flatJsonObjects)
    val writer = CSVWriter.open(csvFile)
    writer.writeRow(headers)
    writer.writeAll(lines)
    writer.close()
  }

  def convertJsonLinesToCsv(jsonLinesFile: File, csvFile: File): Unit = {
    val jsonLines = Files.readAllLines(jsonLinesFile.toPath).toSeq.filterNot(_.isEmpty)
    val flatJsonObjects = jsonLines.map(parse(_)).flatMap(flattenMax)
    val (headers, lines, _) = toCsv(flatJsonObjects)
    val writer = CSVWriter.open(csvFile)
    writer.writeRow(headers)
    writer.writeAll(lines)
    writer.close()
  }

  def extractSchema(jsonFile: File, schemaFile: File): Unit = {
    val schema = JsonSchema.from(parse(jsonFile))
    val writer = new PrintWriter(schemaFile)
    writer.write(pretty(render(JsonSchema.toJsonAst(schema))))
    writer.close()
  }

  def extractMergedSchema(jsonLinesFile: File, schemaFile: File): Unit = {
    val schemas = Files
      .lines(jsonLinesFile.toPath)
      .iterator().asScala.toStream
      .map(line => JsonSchema.from(parse(line)))
      .foldLeft(Seq.empty[JsonSchema])(JsonSchema.merge)

    val writer = new PrintWriter(schemaFile)
    writer.write(pretty(render(JsonSchema.toJsonAst(schemas))))
    writer.close()
  }
}
