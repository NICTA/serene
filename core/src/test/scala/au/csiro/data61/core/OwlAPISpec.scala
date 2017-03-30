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

import au.csiro.data61.core.api.OwlAPI.APIVersion
import au.csiro.data61.core.storage.JsonFormats
import au.csiro.data61.types.SsdTypes.OwlDocumentFormat.{OwlDocumentFormat, Xml, Turtle}
import au.csiro.data61.types.SsdTypes.{Owl, OwlID}
import com.twitter.finagle.http.Method.Delete
import com.twitter.finagle.http.Status.Ok
import com.twitter.finagle.http.{FileElement, Request, RequestBuilder, Status}
import com.twitter.io.Reader
import com.twitter.util.Await
import org.apache.commons.io.FileUtils
import org.json4s.jackson.JsonMethods.parse
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.junit.JUnitRunner

import scala.util.Try

@RunWith(classOf[JUnitRunner])
class OwlAPISpec extends FunSuite with JsonFormats {
  implicit val version = APIVersion

  val RdfXmlDocument = new File(getClass.getResource("/rdf-xml-example.owl").toURI)
  val TurtleDocument = new File(getClass.getResource("/turtle-example.owl").toURI)

  val RdfXmlOwlDescription = "RDF/XML example document"
  val TurtleOwlDescription = "Turtle example document"

  def updateOwl(id: OwlID, document: File, description: String)(implicit server: TestServer): Try[Owl] = Try {

    val buf = Await.result(Reader.readAll(Reader.fromFile(document)))

    val request = RequestBuilder()
      .url(server.fullUrl(s"/$APIVersion/owl/$id"))
      .addFormElement("description" -> description)
      .add(FileElement("file", buf, None, Some(document.getName)))
      .buildFormPost(multipart = true)

    val response = Await.result(server.client(request))
    parse(response.contentString).extract[Owl]
  }

  test("API version number should be 1.0") {
    assert(APIVersion === "v1.0")
  }

  test(s"POSTing to /$APIVersion/owl should create an OWL") (new TestServer {
    try {
      val (status, content) = requestOwlCreation(RdfXmlDocument, Xml, RdfXmlOwlDescription).get
      val owl = parse(content).extract[Owl]

      status should be (Ok)

      owl should have (
        'name (RdfXmlDocument.getName),
        'description (RdfXmlOwlDescription),
        'format (Xml)
      )

      owl.dateCreated should equal (owl.dateModified)
    } finally {
      deleteAllOwls
      assertClose()
    }
  })

  test(s"DELETing /$APIVersion/owl/:id should delete an OWL") (new TestServer {
    try {
      val createdOwl = createOwl(RdfXmlDocument, Xml, RdfXmlOwlDescription).get
      val (status, content) = requestOwlDeletion(createdOwl.id).get
//      val deletedOwl = parse(content).extract[Owl]
      val owls = listOwls.get

      status should be (Ok)
      assert(content.nonEmpty)
//      createdOwl should equal (deletedOwl)
      owls should be (empty)
    } finally {
      deleteAllOwls
      assertClose()
    }
  })

  test(s"GETing /$APIVersion/owl should get a list of IDs of all OWLs") (new TestServer {
    try {
      val rdfXmlOwl = createOwl(RdfXmlDocument, Xml, RdfXmlOwlDescription).get
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
      val createdOwl = createOwl(RdfXmlDocument, Xml, RdfXmlOwlDescription).get
      val owl = getOwl(createdOwl.id).get

      owl should equal(createdOwl)
    } finally {
      deleteAllOwls
      assertClose()
    }
  })

  test(s"POSTing to /$APIVersion/owl/:id should update an OWL") (new TestServer {
    try {
      val createdOwl = createOwl(RdfXmlDocument, Xml, RdfXmlOwlDescription).get
      val updatedDescription = "Updated description"
      val updatedOwl = updateOwl(createdOwl.id, TurtleDocument, updatedDescription).get

      updatedOwl should have (
        'id (createdOwl.id),
        'name (TurtleDocument.getName),
        'description (updatedDescription),
        'format (createdOwl.format),
        'dateCreated (createdOwl.dateCreated)
      )

      updatedOwl.dateModified.getMillis should be > updatedOwl.dateCreated.getMillis
    } finally {
      deleteAllOwls
      assertClose()
    }
  })

  test(s"GETing /$APIVersion/owl/:id/file should get an OWL document") (new TestServer {
    try {
      val createdOwl = createOwl(RdfXmlDocument, Xml, RdfXmlOwlDescription).get
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