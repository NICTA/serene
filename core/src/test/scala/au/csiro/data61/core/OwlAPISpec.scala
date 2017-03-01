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
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files

import au.csiro.data61.core.api.OwlAPI.APIVersion
import au.csiro.data61.core.storage.JsonFormats
import au.csiro.data61.types.SsdTypes.OwlDocumentFormat
import au.csiro.data61.types.SsdTypes.OwlDocumentFormat.OwlDocumentFormat
import au.csiro.data61.types.SsdTypes.OwlDocumentFormat.{RdfXml, Turtle}
import au.csiro.data61.types.SsdTypes.{Owl, OwlDocumentFormat, OwlID}
import com.twitter.finagle.http.Method.Delete
import com.twitter.finagle.http.Status.Ok
import com.twitter.finagle.http.{FileElement, Request, RequestBuilder, Status}
import com.twitter.io.Reader
import com.twitter.util.Await
import org.json4s.jackson.JsonMethods.parse
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.scalatest.Matchers._
import org.apache.commons.io.FileUtils

import scala.util.Try

class OwlAPISpec extends FunSuite with JsonFormats {
  val RdfXmlDocument = new File(getClass.getResource("/rdf-xml-example.owl").toURI)
  val TurtleDocument = new File(getClass.getResource("/turtle-example.owl").toURI)

  val RdfXmlOwlDescription = "RDF/XML example document"
  val TurtleOwlDescription = "Turtle example document"

  def requestOwlCreation(document: File, format: OwlDocumentFormat, description: String)
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

  def createOwl(document: File, format: OwlDocumentFormat, description: String)
    (implicit server: TestServer): Try[Owl] =
    requestOwlCreation(document, format, description).map {
      case (Ok, content) => parse(content).extract[Owl]
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

  def updateOwl(id: OwlID, description: String)(implicit server: TestServer): Try[Owl] = Try {
    val request = RequestBuilder()
      .url(server.fullUrl(s"/$APIVersion/owl/$id"))
      .addFormElement("description" -> description)
      .buildFormPost()
    val response = Await.result(server.client(request))
    parse(response.contentString).extract[Owl]
  }

  test("API version number should be 1.0") {
    assert(APIVersion === "v1.0")
  }

  test(s"POSTing to /$APIVersion/owl should create an OWL") (new TestServer {
    try {
      val (status, content) = requestOwlCreation(RdfXmlDocument, RdfXml, RdfXmlOwlDescription).get
      val owl = parse(content).extract[Owl]

      status should be (Ok)

      owl should have (
        'name (RdfXmlDocument.getName),
        'description (RdfXmlOwlDescription),
        'format (RdfXml)
      )

      owl.dateCreated should equal (owl.dateModified)
    } finally {
      deleteAllOwls
      assertClose()
    }
  })

  test(s"DELETEing /$APIVersion/owl/:id should delete an OWL") (new TestServer {
    try {
      val createdOwl = createOwl(RdfXmlDocument, RdfXml, RdfXmlOwlDescription).get
      val (status, content) = requestOwlDeletion(createdOwl.id).get
      val deletedOwl = parse(content).extract[Owl]
      val owls = listOwls.get

      status should be (Ok)
      createdOwl should equal (deletedOwl)
      owls should be (empty)
    } finally {
      deleteAllOwls
      assertClose()
    }
  })

  test(s"GETing /$APIVersion/owl should get a list of IDs of all OWLs") (new TestServer {
    try {
      val rdfXmlOwl = createOwl(RdfXmlDocument, RdfXml, RdfXmlOwlDescription).get
      val turtleOwl = createOwl(TurtleDocument, Turtle, TurtleOwlDescription).get
      val owls = listOwls.get

      owls should have length 2
      owls should contain (rdfXmlOwl.id)
      owls should contain (turtleOwl.id)
    } finally {
      deleteAllOwls
      assertClose()
    }
  })

  test(s"GETing /$APIVersion/owl/:id should get an OWL") (new TestServer {
    try {
      val createdOwl = createOwl(RdfXmlDocument, RdfXml, RdfXmlOwlDescription).get
      val owl = getOwl(createdOwl.id).get
      owl should equal(createdOwl)
    } finally {
      deleteAllOwls
      assertClose()
    }
  })

  test(s"POSTing to /$APIVersion/owl/:id should update an OWL") (new TestServer {
    try {
      val createdOwl = createOwl(RdfXmlDocument, RdfXml, RdfXmlOwlDescription).get
      val updatedDescription = "Updated description"
      val updatedOwl = updateOwl(createdOwl.id, updatedDescription).get

      updatedOwl should have (
        'id (createdOwl.id),
        'name (createdOwl.name),
        'description (updatedDescription),
        'format (createdOwl.format),
        'dateCreated (createdOwl.dateCreated)
      )

      updatedOwl.dateModified.getMillis should be >= updatedOwl.dateCreated.getMillis
    } finally {
      deleteAllOwls
      assertClose()
    }
  })

  test(s"GETing /$APIVersion/owl/:id/file should get an OWL document") (new TestServer {
    try {
      val createdOwl = createOwl(RdfXmlDocument, RdfXml, RdfXmlOwlDescription).get
      val request = Request(s"/$APIVersion/owl/${createdOwl.id}/file")
      val response = Await.result(client(request))

      response.contentType should equal (Some("text/plain"))
      response.contentString should equal (FileUtils.readFileToString(RdfXmlDocument, UTF_8))
    } finally {
      deleteAllOwls
      assertClose()
    }
  })
}