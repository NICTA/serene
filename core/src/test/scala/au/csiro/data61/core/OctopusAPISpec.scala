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
package au.csiro.data61.core

import java.io.FileInputStream
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import au.csiro.data61.core.storage._
import au.csiro.data61.types.SsdTypes._
import au.csiro.data61.types._
import api._
import au.csiro.data61.core.drivers.Generic._

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FileUtils
import org.joda.time.DateTime

import com.twitter.finagle.http.RequestBuilder
import com.twitter.finagle.http._
import com.twitter.io.Buf
import com.twitter.util.{Await, Return, Throw}

//import scala.concurrent._
import org.scalatest.concurrent._
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.annotation.tailrec
import scala.concurrent.Future
import scala.util.{Failure, Random, Success, Try}

import language.postfixOps

import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._


/**
  * Tests for the OctopusAPI endpoints
  */

class OctopusAPISpec extends FunSuite with JsonFormats with BeforeAndAfterEach with LazyLogging {

  import OctopusAPI._

//  override def afterEach(): Unit = {
//    SsdStorage.removeAll()
//    OctopusStorage.removeAll()
//    DatasetStorage.removeAll()
//    OwlStorage.removeAll()
//    ModelStorage.removeAll()
//  }

  /**
    * Clean all resources from the server
    *
    * @param server Reference to the TestServer used in a single test
    */
  def deleteOctopi()(implicit server: TestServer): Unit = {
    val response = server.get(s"/$APIVersion/octopus")

    if (response.status == Status.Ok) {
      val str = response.contentString
      val regex = "[0-9]+".r
      val models = regex.findAllIn(str).map(_.toInt)
      models.foreach { model =>
        server.delete(s"/$APIVersion/octopus/$model")
      }
    }
  }

  val businessSsdID = 0
  val exampleOwlID = 1

  val DataSet = new DatasetRestAPISpec

  def setUp(): Unit = {
    copySampleDatasets() // copy csv files for getCities and businessInfo
    SsdStorage.add(businessSSD.id, businessSSD) // add businessInfo ssd
    OwlStorage.add(exampleOwl.id, exampleOwl)  // add sample ontology
    // write owl file
    Try{
      val stream = new FileInputStream(exampleOntolPath.toFile)
      OwlStorage.writeOwlDocument(exampleOwlID, stream)
    }

  }

  /**
    * Builds a standard POST request object from a json object for Octopus endpoint.
    *
    * @param json
    * @param url
    * @return
    */
  def postRequest(json: JObject, url: String = s"/$APIVersion/octopus")(implicit s: TestServer): Request = {
    RequestBuilder()
      .url(s.fullUrl(url))
      .addHeader("Content-Type", "application/json")
      .buildPost(Buf.Utf8(compact(render(json))))
  }

  val ssdDir = getClass.getResource("/ssd").getPath

  def readSSD(ssdPath: String): Ssd = {
    Try {
      val stream = new FileInputStream(Paths.get(ssdPath).toFile)
      parse(stream).extract[Ssd]
    } match {
      case Success(ssd) =>
        ssd
      case Failure(err) =>
        fail(err.getMessage)
    }
  }

  val exampleOntolPath = Paths.get(ssdDir,"dataintegration_report_ontology.owl")
  val exampleOwl = Owl(id = exampleOwlID, name = exampleOntolPath.toString, format = OwlDocumentFormat.DefaultOwl,
    description = "sample", dateCreated = DateTime.now, dateModified = DateTime.now)

  val partialSSD: Ssd = readSSD(Paths.get(ssdDir,"partial_model.ssd").toString)
  val veryPartialSSD: Ssd = readSSD(Paths.get(ssdDir,"partial_model2.ssd").toString)
  val emptyCitiesSSD: Ssd = readSSD(Paths.get(ssdDir,"empty_getCities.ssd").toString)
  val emptySSD: Ssd = readSSD(Paths.get(ssdDir,"empty_model.ssd").toString)
  val businessSSD: Ssd = readSSD(Paths.get(ssdDir,"businessInfo.ssd").toString)

  def defaultFeatures: JObject =
    ("activeFeatures" -> Seq("num-unique-vals", "prop-unique-vals", "prop-missing-vals",
      "ratio-alpha-chars", "prop-numerical-chars",
      "prop-whitespace-chars", "prop-entries-with-at-sign",
      "prop-entries-with-hyphen", "prop-entries-with-paren",
      "prop-entries-with-currency-symbol", "mean-commas-per-entry",
      "mean-forward-slashes-per-entry",
      "prop-range-format", "is-discrete", "entropy-for-discrete-values")) ~
      ("activeFeatureGroups" -> Seq("stats-of-text-length", "prop-instances-per-class-in-knearestneighbours")) ~
      ("featureExtractorParams" -> Seq(
        ("name" -> "prop-instances-per-class-in-knearestneighbours") ~
          ("num-neighbours" -> 5)))

  def randomString: String = Random.alphanumeric take 10 mkString

  val helperDir = getClass.getResource("/helper").getPath
  val sampleDsDir = getClass.getResource("/sample.datasets").getPath
  val datasetMap = Map("businessInfo" -> 767956483, "getCities" -> 696167703)
  val businessDsPath = Paths.get(sampleDsDir,
    datasetMap("businessInfo").toString, datasetMap("businessInfo").toString + ".json")
  val citiesDsPath = Paths.get(sampleDsDir,
    datasetMap("getCities").toString, datasetMap("getCities").toString + ".json")

  /**
    * Manually add datasets to the storage layer since SSDs have hard-coded mappings for columns.
    */
  def copySampleDatasets(): Unit = {
    // copy sample dataset to Config.DatasetStorageDir
    if (!Paths.get(Serene.config.storageDirs.dataset).toFile.exists) { // create dataset storage dir
      Paths.get(Serene.config.storageDirs.dataset).toFile.mkdirs}
    val dsDir = Paths.get(sampleDsDir).toFile // directory to copy from
    FileUtils.copyDirectory(dsDir,                    // copy sample dataset
      Paths.get(Serene.config.storageDirs.dataset).toFile)

    // adding datasets explicitly to the storage
    val businessDS: DataSet = Try {
      val stream = new FileInputStream(businessDsPath.toFile)
      parse(stream).extract[DataSet]
    } match {
      case Success(ds) => ds
      case Failure(err) => fail(err.getMessage)
    }
    val citiesDS: DataSet = Try {
      val stream = new FileInputStream(businessDsPath.toFile)
      parse(stream).extract[DataSet]
    } match {
      case Success(ds) => ds
      case Failure(err) => fail(err.getMessage)
    }

    DatasetStorage.add(businessDS.id, businessDS)
    DatasetStorage.add(citiesDS.id, citiesDS)
  }

  test("GET /v1.0/octopus responds Ok(200)") (new TestServer {
    try {
      val response = get(s"/$APIVersion/octopus")
      assert(response.contentType === Some(JsonHeader))
      assert(response.status === Status.Ok)
      assert(!response.contentString.isEmpty)
      assert(Try { parse(response.contentString).extract[List[OctopusID]] }.isSuccess)
    } finally {
      deleteOctopi()
      DataSet.deleteAllDataSets()
      assertClose()
    }
  })

  test("POST /v1.0/octopus responds BadRequest") (new TestServer {
    try {
      val TestStr = randomString
      val randomInt = genID
      val dummySeqInt = List(1, 2, 3)

      val json =
      ("description" -> TestStr) ~
        ("name" -> "very fancy name here") ~
        ("modelType" -> "randomForest") ~
        ("features" -> defaultFeatures) ~
        ("resamplingStrategy" -> "NoResampling") ~
        ("numBags" -> randomInt) ~
        ("bagSize" -> randomInt) ~
        ("ssds" -> dummySeqInt) ~
        ("ontologies" -> dummySeqInt) ~
        ("modelingProps" -> TestStr)


      val request = postRequest(json)

      val response = Await.result(client(request))

      println(response.status)
      println(response.contentString)
      assert(response.contentType === Some(JsonHeader))
      assert(response.status === Status.BadRequest)
      assert(!response.contentString.isEmpty)

    } finally {
      deleteOctopi()
      assertClose()
    }
  })
}
