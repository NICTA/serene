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

import java.io.File
import java.nio.file.Paths

import au.csiro.data61.core.storage._
import au.csiro.data61.types.SsdTypes._
import au.csiro.data61.types._
import api._
import au.csiro.data61.core.api.OctopusAPI.{APIVersion => _, formats => _, _}
import au.csiro.data61.core.api.SsdAPI._
import au.csiro.data61.types.ColumnTypes.ColumnID
import com.twitter.finagle.http.Method.Delete
import com.typesafe.scalalogging.LazyLogging
import com.twitter.finagle.http.RequestBuilder
import com.twitter.finagle.http._
import com.twitter.io.Buf
import com.twitter.util.Await
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

//import scala.concurrent._
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.annotation.tailrec
import scala.concurrent.Future
import scala.util.Try

import language.postfixOps

import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._


/**
  * Tests for the coordination between different storage layers and apis.
  */
@RunWith(classOf[JUnitRunner])
class CoordinationSpec  extends FunSuite with JsonFormats with BeforeAndAfterEach with LazyLogging {

  implicit val version = APIVersion

  val OctopusSpec = new OctopusAPISpec

  val SsdDocument = new File(getClass.getResource("/ssd/request.ssd").toURI)
  val DatasetDocument = new File(getClass.getResource("/tiny.csv").toURI)

  val sampleDsDir = getClass.getResource("/sample.datasets").getPath
  val ssdDir = getClass.getResource("/ssd").getPath
  val owlDir = getClass.getResource("/owl").getPath
  val datasetMap = Map("businessInfo" -> 767956483, "getCities" -> 696167703)

  val businessDs = Paths.get(sampleDsDir,
    datasetMap("businessInfo").toString, "businessinfo.csv").toFile
  val businessSsd = Paths.get(ssdDir, "businessInfo.ssd").toFile

  val citiesDs = Paths.get(sampleDsDir,
    datasetMap("getCities").toString, "getcities.csv").toFile
  val citiesSsd = Paths.get(ssdDir, "getCities.ssd").toFile

  val exampleOwl = Paths.get(owlDir, "dataintegration_report_ontology.ttl").toFile
  val exampleOwlFormat = OwlDocumentFormat.Turtle

  val museumDs = Paths.get(getClass.getResource("/helper/s27-s-the-huntington.json.csv").getPath).toFile
  val inconsistentSsd = Paths.get(ssdDir, "inconsistent_s27.ssd").toFile

  lazy val PollTime = 2000
  lazy val PollIterations = 20

  /**
    * Helper function to convert from Ssd type to SsdRequest.
    *
    * @param ssd Semantic Source Description
    * @return
    */
  def convertSsd(ssd: Ssd): SsdRequest = {
    SsdRequest(Some(ssd.name), Some(ssd.ontologies), ssd.semanticModel, ssd.mappings)
  }

  /**
    * Binds ssd from the json file to a csv file using ontologies.
    * @param datasetDocument
    * @param ssdDocument
    * @param ontologies
    * @param server
    * @return
    */
  def bindSsd(datasetDocument: File,
              ssdDocument: File,
              ontologies: List[OwlID])(implicit server: TestServer): Try[SsdRequest] = Try {
    val dataset = server.createDataset(datasetDocument, "ref dataset").get
    val originalSsd = parse(ssdDocument).extract[Ssd]

    val attrNameMap: Map[AttrID, String] = originalSsd.attributes
      .map {
        attr => attr.id -> attr.name
      } toMap


    val colNameMap: Map[String, ColumnID] = dataset.columns.map { c => c.name -> c.id } toMap

    val newMappings: Option[SsdMapping] = originalSsd.mappings
      .map {
        maps =>
          SsdMapping(maps.mappings
            .map {
              case (aID, nID) => (colNameMap(attrNameMap(aID)), nID)
            })
      }

    println(s"new mappings: ${newMappings.get.mappings.size}")

    val newSsd = originalSsd.copy(ontologies = ontologies, mappings = newMappings)

    convertSsd(newSsd)
  }


  /**
    * Binds ssd from the json file to a csv file using ontologies and then upload
    * @param datasetDocument
    * @param ssdDocument
    * @param ontologies
    * @param server
    * @return
    */
  def createBoundSsd(datasetDocument: File,
                     ssdDocument: File,
                     ontologies: List[OwlID])(implicit server: TestServer): Try[Ssd] = {
    for {
      request <- bindSsd(datasetDocument, ssdDocument, ontologies)
      ssd <- server.createSsd(request)
    } yield ssd
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

  /**
    * Creates default octopus
    *
    * @param s
    * @return
    */
  def createOctopus(trainSsd: List[SsdID], owls: List[OwlID])(implicit s: TestServer): Octopus = {

    val json = ("ssds" -> trainSsd) ~
      ("ontologies" -> owls)

    val request = postRequest(json)
    val response = Await.result(s.client(request))
    assert(response.status === Status.Ok)

    // created octopus
    parse(response.contentString).extract[Octopus]
  }

  def trainOctopus(octopus: Octopus)(implicit s: TestServer): Octopus = {

    val req = postRequest(json = JObject(), url = s"/$APIVersion/octopus/${octopus.id}/train")
    // send the request and make sure it executes
    val resp = Await.result(s.client(req))

    assert(resp.status === Status.Accepted)
    assert(resp.contentString.isEmpty)

    octopus
  }

  /**
    * pollOctopusState
    *
    * @param model
    * @param pollIterations
    * @param pollTime
    * @param s
    * @return
    */
  def pollOctopusState(model: Octopus, pollIterations: Int, pollTime: Int)(implicit s: TestServer)
  : Future[Training.Status] = {
    Future {

      def state(): Training.Status = {
        Thread.sleep(pollTime)
        // build a request to get the model...
        val response = s.get(s"/$APIVersion/octopus/${model.id}")
        if (response.status != Status.Ok) {
          throw new Exception("Failed to retrieve model state")
        }
        // ensure that the data is correct...
        val m = parse(response.contentString).extract[Octopus]

        m.state.status
      }

      @tailrec
      def rState(loops: Int): Training.Status = {
        state() match {
          case s@Training.Status.COMPLETE =>
            s
          case s@Training.Status.ERROR =>
            s
          case _ if loops < 0 =>
            throw new Exception("Training timeout")
          case _ =>
            rState(loops - 1)
        }
      }

      rState(pollIterations)
    }
  }

  /**
    * Clean all octopi
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

  //=========================Tests==============================================
  test(s"DELETE /$APIVersion/dataset/:id should fail due to ssd dependent") (new TestServer {
    try {
      val createdSsd = createSsd(DatasetDocument, SsdDocument).get._2
      val dsId = getAllDatasets.head

      val response = deleteDataset(dsId)

      assert(response.status === Status.BadRequest)
      assert(response.contentString.nonEmpty)
      assert(response.contentString.contains(createdSsd.id.toString))
    } finally {
      deleteOctopi
      deleteAllSsds
      deleteAllDatasets
      assertClose()
    }
  })

  test("POST cities ssd responds Ok")(new TestServer {
    try {
      val createdOwl: Owl = createOwl(exampleOwl, exampleOwlFormat).get
      val createdSsd: Ssd = createBoundSsd(citiesDs, citiesSsd, List(createdOwl.id)).get

      assert(createdSsd.isComplete)
      assert(createdSsd.isConsistent)

    } finally {
      deleteOctopi
      deleteAllSsds
      deleteAllDatasets
      deleteAllOwls
      assertClose()
    }
  })

  test("DELETE owl responds BadRequest due to dependent ssd")(new TestServer {
    try {
      val createdOwl: Owl = createOwl(exampleOwl, exampleOwlFormat).get
      val createdSsd: Ssd = createBoundSsd(citiesDs, citiesSsd, List(createdOwl.id)).get

      val (status, response)  = requestOwlDeletion(createdOwl.id).get
      assert(status === Status.BadRequest)

      println(response)
      assert(response.contains(createdSsd.id.toString))

    } finally {
      deleteOctopi
      deleteAllSsds
      deleteAllDatasets
      deleteAllOwls
      assertClose()
    }
  })

  test("POST training for simple octopus accepted and completed")(new TestServer {
    try {
      val createdOwl: Owl = createOwl(exampleOwl, exampleOwlFormat).get
      val createdSsd: Ssd = createBoundSsd(businessDs, businessSsd, List(createdOwl.id)).get

      val octopus = createOctopus(List(createdSsd.id), List(createdOwl.id))

      assert(createdSsd.isComplete)
      assert(createdSsd.isConsistent)
      assert(octopus.ontologies === List(createdOwl.id))
      assert(octopus.ssds === List(createdSsd.id))

      val octo = trainOctopus(octopus)

      val trained = pollOctopusState(octopus, PollIterations, PollTime)
      val state = concurrent.Await.result(trained, PollIterations * PollTime *2 seconds)
      assert(state === Training.Status.COMPLETE)

      // get the model state
      assert(ModelStorage.get(octopus.lobsterID).nonEmpty)
      val model = ModelStorage.get(octopus.lobsterID).get
      assert(model.state.status === Training.Status.COMPLETE)

    } finally {
      deleteOctopi
      deleteAllSsds
      deleteAllDatasets
      deleteAllOwls
      assertClose()
    }
  })

  test("DELETE model responds BadRequest due to dependent octopus")(new TestServer {
    try {
      val createdOwl: Owl = createOwl(exampleOwl, exampleOwlFormat).get
      val createdSsd: Ssd = createBoundSsd(businessDs, businessSsd, List(createdOwl.id)).get

      val octopus = createOctopus(List(createdSsd.id), List(createdOwl.id))

      val response = deleteModel(octopus.lobsterID)

      assert(response.status === Status.BadRequest)
      assert(response.contentString.contains(octopus.id.toString))

    } finally {
      deleteOctopi
      deleteAllSsds
      deleteAllDatasets
      deleteAllOwls
      assertClose()
    }
  })

  //==============================================================================
//  test("POST museum ssd responds Ok") (new TestServer {
//    try {
//      // ssd is inconsistent, but bindSsd does some magic!
//      val resp = for {
//        createdOwl <- createOwl(exampleOwl, exampleOwlFormat)
//        createdSsd <- bindSsd(museumDs, inconsistentSsd, List(createdOwl.id))
//        response <- requestSsdCreation(createdSsd)
//      } yield response
//
//      println(resp)
//      assert(resp.isSuccess)
//      assert(resp.get._1 === Status.Ok)
//      assert(resp.get._2.nonEmpty)
//
//      val createdSsd = Try{parse(resp.get._2).extract[Ssd]}
//      assert(createdSsd.isSuccess)
//
//    } finally {
//      //      deleteAllSsds
//      deleteAllDatasets
//      assertClose()
//    }
//  })


}

