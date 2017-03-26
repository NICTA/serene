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

import java.nio.file.Paths

import org.specs2._
import au.csiro.data61.matcher.data._
import au.csiro.data61.matcher.ingestion.loader._

class CSVDataLoaderSpec extends mutable.Specification {
    val helperDir = getClass.getResource("/datasets").getPath
    val sampleData = "src/test/resources/datasets/simpleCsvData/db1"

    // s"""CSVDataLoader load()""" should {
    //     s"load and parse csv data" in {
    //         val dataset = CSVDataLoader("testdb").load(sampleData)
    //         // val children = dataset.children.get
    //         println(dataset)

    //         1 mustEqual 1
    //     }
    // }

  s"""CSVDataLoader load centrelink-locations.csv""" should {
    s"load and parse csv data" in {
      val dataset = CsvDataLoader("testdb").load(Paths.get(helperDir, "data.gov.au", "db5").toString)
      val headers = DataModel.getAllAttributes(dataset).map(_.metadata.get.name)
      val values = DataModel.getAllAttributes(dataset).map(_.values.size)

      dataset.id mustEqual "testdb"
      dataset.metadata mustEqual Some(Metadata(name="CSV Dataset", description="CSV Dataset"))
      headers mustEqual List("id", "Terminal Name", "Street",
        "Location", "Cross Street", "Suburb", "Latitude", "Longitude",
        "Accessibility", "Connecting Bus Services", "Parking")

//      println("=================values")
//      println(values)
//      println("================")
      values.forall(_ == 25) mustEqual true
    }
  }

  s"""CSVDataLoader loads no_header.csv""" should {
    s"create attributes with no Metadata" in {
      val dataset = CsvDataLoader("no_header").load(Paths.get(helperDir, "no_header.csv").toString)
      val headers = DataModel.getAllAttributes(dataset).map(_.metadata)
      val values = DataModel.getAllAttributes(dataset).map(_.values.size)

//      println("================")
//      println(s"headers: $headers")
//      println(s"values: $values")
//      println("================")
      headers mustEqual List.fill(11)(None)
      values.forall(_ == 11) mustEqual true
    }
  }
}
