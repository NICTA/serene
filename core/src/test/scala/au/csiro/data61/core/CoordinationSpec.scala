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

import java.io.{File, FileInputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import au.csiro.data61.core.storage._
import au.csiro.data61.types.SsdTypes._
import au.csiro.data61.types._
import api._
import au.csiro.data61.core.api.SsdAPI._
import au.csiro.data61.types.ColumnTypes.ColumnID
import au.csiro.data61.types.DataSetTypes._
import au.csiro.data61.types.SsdTypes.OwlDocumentFormat._
import com.twitter.finagle.http.Method.{Delete, Post}
import com.typesafe.scalalogging.LazyLogging
import com.twitter.finagle.http.RequestBuilder
import com.twitter.finagle.http.Status._
import com.twitter.finagle.http._
import com.twitter.io.Buf.ByteArray
import com.twitter.io.{Buf, Reader}
import com.twitter.util.{Await, Return, Throw}
import org.json4s.jackson.Serialization._

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
  * Tests for the coordination between different storage layers and apis.
  */
class CoordinationSpec  extends FunSuite with JsonFormats with BeforeAndAfterEach with LazyLogging {

  val SsdDocument = new File(getClass.getResource("/ssd/request.ssd").toURI)
  val DatasetDocument = new File(getClass.getResource("/tiny.csv").toURI)

  val sampleDsDir = getClass.getResource("/sample.datasets").getPath
  val ssdDir = getClass.getResource("/ssd").getPath
  val owlDir = getClass.getResource("/owl").getPath
  val datasetMap = Map("businessInfo" -> 767956483, "getCities" -> 696167703)

  val businessDs = Paths.get(sampleDsDir,
    datasetMap("businessInfo").toString, "businessinfo.csv").toFile
  val businessSsd = Paths.get(ssdDir, "businessinfo.ssd").toFile

  val citiesDs = Paths.get(sampleDsDir,
    datasetMap("getCities").toString, "getcities.csv").toFile
  val citiesSsd = Paths.get(ssdDir, "getCities.ssd").toFile


  val exampleOwl = Paths.get(owlDir, "dataintegration_report_ontology.owl").toFile
  val exampleOwlFormat = OwlDocumentFormat.Turtle

  def requestOwlCreation(document: File, format: OwlDocumentFormat, description: String = "test")
                        (implicit server: TestServer): Try[(Status, String)] = Try {
    val buf = Await.result(Reader.readAll(Reader.fromFile(document)))
    val request = RequestBuilder()
      .url(server.fullUrl(s"/$APIVersion/owl"))
      .addFormElement("format" -> format.toString)
      .addFormElement("description" -> description)
      .add(FileElement("file", buf, None, Some(document.getName)))
      .buildFormPost(multipart = true)
    val response = Await.result(server.client(request))
    (response.status, response.contentString)
  }

  def createOwl(document: File, format: OwlDocumentFormat, description: String = "example")
               (implicit server: TestServer): Try[Owl] =
    requestOwlCreation(document, format, description).map {
      case (Ok, content) => parse(content).extract[Owl]
    }

  def createDataset(document: File, description: String="test")
                   (implicit server: TestServer): Try[DataSet] = Try {
    val buf = Await.result(Reader.readAll(Reader.fromFile(document)))
    val request = RequestBuilder()
      .url(server.fullUrl(s"/$APIVersion/dataset"))
      .addFormElement("description" -> description)
      .add(FileElement("file", buf, None, Some(document.getName)))
      .buildFormPost(multipart = true)
    val response = Await.result(server.client(request))
    parse(response.contentString).extract[DataSet]
  }

  def deleteDataset(id: DataSetID)(implicit server: TestServer): Response = {
    logger.info(s"Deleting dataset $id")
    val request = Request(Delete, s"/$APIVersion/dataset/$id")
    Await.result(server.client(request))
  }

  def deleteAllDatasets(implicit server: TestServer): Unit = {
    val request = Request(s"/$APIVersion/dataset")
    val response = Await.result(server.client(request))
    parse(response.contentString).extract[List[DataSetID]].foreach(deleteDataset)
  }

  def getAllDatasets(implicit server: TestServer): List[DataSetID] = {
    val request = Request(s"/$APIVersion/dataset")
    val response = Await.result(server.client(request))
    parse(response.contentString).extract[List[DataSetID]]
  }

  def requestSsdCreation(document: SsdRequest)
                        (implicit server: TestServer): Try[(Status, String)] = Try {
    val request = Request(Post, s"/$APIVersion/ssd")
    request.content = ByteArray(write(document).getBytes: _*)
    request.contentType = "application/json"
    val response = Await.result(server.client(request))
    (response.status, response.contentString)
  }

  def createSsd(document: SsdRequest)(implicit server: TestServer): Try[Ssd] = Try {
    val (_, content) = requestSsdCreation(document).get
    parse(content).extract[Ssd]
  }

  def createSsd(datasetDocument: File, ssdDocument: File)
               (implicit server: TestServer): Try[(SsdRequest, Ssd)] = Try {
    val dataset = createDataset(datasetDocument, "ref dataset").get
    val request = parse(ssdDocument).extract[SsdRequest].copy(mappings = Some(SsdMapping(Map(
      dataset.columns.head.id -> 1,
      dataset.columns(1).id -> 3,
      dataset.columns(2).id -> 5,
      dataset.columns(3).id -> 7
    ))))
    (request, createSsd(request).get)
  }

  def requestOwlDeletion(id: OwlID)(implicit server: TestServer): Try[(Status, String)] = Try {
    val request = Request(Delete, s"/$APIVersion/owl/$id")
    val response = Await.result(server.client(request))
    (response.status, response.contentString)
  }

  def listOwls(implicit server: TestServer): Try[List[OwlID]] = Try {
    val request = Request(s"/$APIVersion/owl")
    val response = Await.result(server.client(request))
    parse(response.contentString).extract[List[OwlID]]
  }

  def deleteAllOwls(implicit server: TestServer): Unit =
    listOwls.get.map(requestOwlDeletion).foreach(_.get)

  def getOwl(id: OwlID)(implicit server: TestServer): Try[Owl] = Try {
    val request = Request(s"/$APIVersion/owl/$id")
    val response = Await.result(server.client(request))
    parse(response.contentString).extract[Owl]
  }

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
              ontologies: List[OwlID])(implicit server: TestServer): Try[Ssd] = Try {
    val dataset = createDataset(datasetDocument, "ref dataset").get
    val originalSsd = parse(ssdDocument).extract[Ssd]

    val attrNameMap: Map[AttrID, String] = originalSsd.attributes
      .map {
        attr => attr.id -> attr.name
      } toMap
    val colNameMap: Map[String, ColumnID] = dataset.columns.map { c => c.name -> c.id} toMap
    val newMappings: Option[SsdMapping] = originalSsd.mappings
      .map {
        maps =>
          SsdMapping(maps.mappings
            .map {
              case (aID,nID) => (colNameMap(attrNameMap(aID)), nID)
            })
      }

    val newSsd = originalSsd.copy(ontologies = ontologies, mappings = newMappings)

    val request = convertSsd(originalSsd)
    createSsd(request).get
  }

  def listSsds(implicit server: TestServer): Try[List[SsdID]] = Try {
    val request = Request(s"/$APIVersion/ssd")
    val response = Await.result(server.client(request))
    parse(response.contentString).extract[List[SsdID]]
  }

  def requestSsdDeletion(id: SsdID)(implicit server: TestServer): Try[(Status, String)] = Try {
    val request = Request(Delete, s"/$APIVersion/ssd/$id")
    val response = Await.result(server.client(request))
    (response.status, response.contentString)
  }

  def deleteAllSsds(implicit server: TestServer): Unit =
    listSsds.get.map(requestSsdDeletion).foreach(_.get)

  def getSsd(id: SsdID)(implicit server: TestServer): Try[Ssd] = Try {
    val request = Request(s"/$APIVersion/ssd/$id")
    val response = Await.result(server.client(request))
    parse(response.contentString).extract[Ssd]
  }

  test(s"DELETE /$APIVersion/dataset/:id should fail due to ssd dependent") (new TestServer {
    try {
      val createdSsd = createSsd(DatasetDocument, SsdDocument).get._2
      val dsId = getAllDatasets.head

      val response = deleteDataset(dsId)

      assert(response.status === Status.BadRequest)
      assert(response.contentString.nonEmpty)
      assert(response.contentString.contains(createdSsd.id.toString))
    } finally {
      deleteAllSsds
      deleteAllDatasets
      assertClose()
    }
  })

  test("POST cities ssd succeeds")(new TestServer {
    try {
      val createdOwl: Owl = createOwl(exampleOwl, exampleOwlFormat).get
      val createdSsd: Ssd = bindSsd(citiesDs, citiesSsd, List(createdOwl.id)).get

      assert(createdSsd.isComplete)
      assert(createdSsd.isConsistent)

    } finally {
      deleteAllSsds
      deleteAllDatasets
      deleteAllOwls
      assertClose()
    }
  })

}
