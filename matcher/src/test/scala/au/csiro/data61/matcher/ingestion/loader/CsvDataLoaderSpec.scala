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
import com.typesafe.scalalogging.LazyLogging

class CsvDataLoaderSpec extends mutable.Specification with LazyLogging{
    val helperDir = getClass.getResource("/datasets").getPath
    val sampleData = "src/test/resources/datasets/simpleCsvData/db1"


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

      values.forall(_ mustEqual  25)
    }
  }

  s"""CSVDataLoader loads no_header.csv""" should {
    s"create attributes with no Metadata" in {
      val dataset = CsvDataLoader("no_header").load(Paths.get(helperDir, "no_header.csv").toString)
      val headers = DataModel.getAllAttributes(dataset).map(_.metadata)
      val values = DataModel.getAllAttributes(dataset).map(_.values.size)

      headers mustEqual List.fill(11)(None)
      values.forall(_ mustEqual  11)
    }
  }

  s"""CSVDataLoader loads tiny.csv""" should {
    s"load and parse empty values" in {
      val dataset = CsvDataLoader("tiny").load(Paths.get(helperDir, "tiny.csv").toString)
      val headers = DataModel.getAllAttributes(dataset).map(_.metadata.get.name)
      val values = DataModel.getAllAttributes(dataset).map(_.values.size)

      headers mustEqual List("A", "B", "C", "D", "E")
      values.forall(_ mustEqual 3)
    }
  }

  s"""CSVDataLoader loads tiny_emptyrows.csv""" should {
    s"filter empty rows" in {
      val dataset = CsvDataLoader("tiny").load(Paths.get(helperDir, "tiny_emptyrows.csv").toString)
      val headers = DataModel.getAllAttributes(dataset).map(_.metadata.get.name)
      val values = DataModel.getAllAttributes(dataset).map(_.values.size)

      headers mustEqual List("A", "B", "C", "D", "E")
      values.forall(_ mustEqual  3)
    }
  }

  s"""CSVDataLoader loads tiny_emptyvals.csv""" should {
    s"filter rows with all values empty" in {
      val dataset = CsvDataLoader("tiny").load(Paths.get(helperDir, "tiny_emptyvals.csv").toString)
      val headers = DataModel.getAllAttributes(dataset).map(_.metadata.get.name)
      val values = DataModel.getAllAttributes(dataset).map(_.values.size)

      headers mustEqual List("A", "B", "C", "D", "E")
      values.forall(_ mustEqual 3)
    }
  }

  s"""CSVDataLoader loads museum csv file""" should {
    s"load and parse" in {
      val dataset = CsvDataLoader("museum").load(Paths.get(helperDir,
        "s14-s-california-african-american.json.csv").toString)
      val headers = DataModel.getAllAttributes(dataset).map(_.metadata.get.name)
      val values = DataModel.getAllAttributes(dataset).map(_.values.size)

      values.forall(_ mustEqual  5)
      headers.size mustEqual 12
    }
  }

  s"""CSVDataLoader loads s28 museum csv file""" should {
    s"load and parse" in {
      val dataset = CsvDataLoader("museum").load(Paths.get(helperDir,
        "s28-wildlife-art.csv.csv").toString)
      val headers = DataModel.getAllAttributes(dataset).map(_.metadata.get.name)
      val values = DataModel.getAllAttributes(dataset).map(_.values.size)

      values.forall(_ mustEqual  7)
      headers.size mustEqual 56
    }
  }
}
