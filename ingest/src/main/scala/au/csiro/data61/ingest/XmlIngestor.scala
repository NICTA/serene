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

import au.csiro.data61.ingest.JsonTransforms.{appendNullFields, flatten, sortFieldsByKeys, toCompactFields}
import com.github.tototoshi.csv.CSVWriter
import org.json4s.JObject
import org.json4s.Xml.toJson

import scala.xml.XML.loadFile

object XmlIngestor {
  def convertToCsv(xmlFile: File, csvFile: File, flattenDepth: Int): Unit = {
    val flattenToDepth: (Seq[JObject]) => Seq[JObject] =
      Function.chain(Seq.fill(flattenDepth){flatten})

    val xml = loadFile(xmlFile)
    val json = toJson(xml).asInstanceOf[JObject]

    val jsonObjs = flattenToDepth(Seq(json))
    val keys = jsonObjs.flatMap(_.obj.map(_._1)).toSet
    val lines = jsonObjs map { jsonObj =>
      val objKeys = jsonObj.obj.map(_._1).toSet
      val fields = toCompactFields(sortFieldsByKeys(appendNullFields(jsonObj.obj, keys &~ objKeys)))
      fields.map(_._2)
    }

    val writer = CSVWriter.open(csvFile)
    writer.writeRow(keys.toSeq.sorted)
    writer.writeAll(lines)
    writer.close()
  }
}
