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

import au.csiro.data61.core.api.SsdAPI.APIVersion
import au.csiro.data61.core.api.SsdRequest
import au.csiro.data61.core.storage.JsonFormats
import au.csiro.data61.types.DataSetTypes.DataSetID
import au.csiro.data61.types.SsdTypes.SsdID
import au.csiro.data61.types.{DataSet, Ssd, SsdMapping}
import com.twitter.finagle.http.Method.{Delete, Post}
import com.twitter.finagle.http.Status.Ok
import com.twitter.finagle.http.{FileElement, Request, RequestBuilder, Status}
import com.twitter.io.Buf.ByteArray
import com.twitter.io.Reader
import com.twitter.util.Await
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization.write
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.junit.JUnitRunner

import scala.util.Try

@RunWith(classOf[JUnitRunner])
class SsdAPISpec extends FunSuite with JsonFormats {
  val SsdDocument = new File(getClass.getResource("/ssd/request.ssd").toURI)
  val DatasetDocument = new File(getClass.getResource("/tiny.csv").toURI)

  def createDataset(document: File, description: String)
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

  def deleteDataset(id: DataSetID)(implicit server: TestServer): Unit = {
    val request = Request(Delete, s"/$APIVersion/dataset/$id")
    Await.result(server.client(request))
  }

  def deleteAllDatasets(implicit server: TestServer): Unit = {
    val request = Request(s"/$APIVersion/dataset")
    val response = Await.result(server.client(request))
    parse(response.contentString).extract[List[DataSetID]].foreach(deleteDataset)
  }

  def createSsd(document: SsdRequest)(implicit server: TestServer): Try[Ssd] = Try {
    val request = Request(Post, s"/$APIVersion/ssd")
    request.content = ByteArray(write(document).getBytes: _*)
    request.contentType = "application/json"
    val response = Await.result(server.client(request))
    parse(response.contentString).extract[Ssd]
  }

  def createSsd(datasetDocument: File, ssdDocument: File)
      (implicit server: TestServer): Try[(SsdRequest, Ssd)] = Try {
    val dataset = createDataset(datasetDocument, "ref dataset").get
    val request = parse(ssdDocument).extract[SsdRequest].copy(
      mappings = Some(SsdMapping(Map(
        dataset.columns.head.id -> 1,
        dataset.columns(1).id -> 3,
        dataset.columns(2).id -> 5,
        dataset.columns(3).id -> 7
      )))
    )
    (request, createSsd(request).get)
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

  def updateSsd(document: SsdRequest, id: SsdID)(implicit server: TestServer): Try[Ssd] = Try {
    val request = Request(Post, s"/$APIVersion/ssd/$id")
    request.content = ByteArray(write(document).getBytes: _*)
    request.contentType = "application/json"
    val response = Await.result(server.client(request))
    parse(response.contentString).extract[Ssd]
  }

  test("API version number should be 1.0") {
    assert(APIVersion === "v1.0")
  }

  test(s"POSTing to /$APIVersion/ssd should create an SSD") (new TestServer {
    try {
      val (request, ssd) = createSsd(DatasetDocument, SsdDocument).get

      ssd should have (
        'name (request.name),
        'ontology (request.ontologies),
        'semanticModel (request.semanticModel),
        'mappings (request.mappings)
      )
      ssd.dateCreated should equal (ssd.dateModified)
    } finally {
      deleteAllSsds
      deleteAllDatasets
      assertClose()
    }
  })

  test(s"DELETEing /$APIVersion/ssd/:id should delete an SSD") (new TestServer {
    try {
      val createdSsd = createSsd(DatasetDocument, SsdDocument).get._2
      val (status, content) = requestSsdDeletion(createdSsd.id).get
//      val deletedSsd = parse(content).extract[Ssd]
      val ssds = listSsds.get

      status should be (Ok)
//      createdSsd should equal (deletedSsd)
      ssds should be (empty)
    } finally {
      deleteAllSsds
      deleteAllDatasets
      assertClose()
    }
  })

  test(s"GETing /$APIVersion/ssd should get a list of IDs of all SSDs") (new TestServer {
    try {
      val ssd1 = createSsd(DatasetDocument, SsdDocument).get._2
      val ssd2 = createSsd(DatasetDocument, SsdDocument).get._2
      val ssds = listSsds.get

      ssds should have length 2
      ssds should contain (ssd1.id)
      ssds should contain (ssd2.id)
    } finally {
      deleteAllSsds
      deleteAllDatasets
      assertClose()
    }
  })

  test(s"GETing /$APIVersion/ssd/:id should get an SSD") (new TestServer {
    try {
      val createdSsd = createSsd(DatasetDocument, SsdDocument).get._2
      val ssd = getSsd(createdSsd.id).get

      ssd should equal (createdSsd)
    } finally {
      deleteAllSsds
      deleteAllDatasets
      assertClose()
    }
  })

  test(s"POSTing to /$APIVersion/ssd/:id should update an SSD") (new TestServer {
    try {
      val createdSsd = createSsd(DatasetDocument, SsdDocument).get._2
      val updatedName = "updated name"
      val request = SsdRequest(
        name = updatedName,
        ontologies = createdSsd.ontology,
        semanticModel = createdSsd.semanticModel,
        mappings = createdSsd.mappings
      )
      val updatedSsd = updateSsd(request, createdSsd.id).get

      updatedSsd should have (
        'id (createdSsd.id),
        'name (updatedName),
        'attributes (createdSsd.attributes),
        'ontology (createdSsd.ontology),
        'semanticModel (createdSsd.semanticModel),
        'mappings (createdSsd.mappings),
        'dateCreated (createdSsd.dateCreated)
      )

      updatedSsd.dateModified.getMillis should be >= updatedSsd.dateCreated.getMillis
    } finally {
      deleteAllSsds
      deleteAllDatasets
      assertClose()
    }
  })

  //TODO: tests for different inconsistent ssds
  //TODO: tests for different updates of ssds
}
