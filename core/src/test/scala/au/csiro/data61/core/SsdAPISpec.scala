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
import au.csiro.data61.types.SsdTypes.SsdID
import au.csiro.data61.types.Ssd
import com.twitter.finagle.http.Method.Post
import com.twitter.finagle.http.Status.{BadRequest, NotFound, Ok}
import com.twitter.finagle.http.{Request, Status}
import com.twitter.io.Buf.ByteArray
import com.twitter.util.Await

import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization.write
import org.json4s._

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.junit.JUnitRunner

import scala.util.Try

@RunWith(classOf[JUnitRunner])
class SsdAPISpec extends FunSuite with JsonFormats {
  implicit val version =APIVersion

  val SsdDocument = new File(getClass.getResource("/ssd/request.ssd").toURI)
  val DatasetDocument = new File(getClass.getResource("/tiny.csv").toURI)


  def requestSsdUpdate(document: SsdRequest, id: SsdID)
      (implicit server: TestServer): Try[(Status, String)] = Try {
    val request = Request(Post, s"/$APIVersion/ssd/$id")
    request.content = ByteArray(write(document).getBytes: _*)
    request.contentType = "application/json"
    val response = Await.result(server.client(request))
    (response.status, response.contentString)
  }

  def updateSsd(document: SsdRequest, id: SsdID)(implicit server: TestServer): Try[Ssd] = Try {
    val (_, content) = requestSsdUpdate(document, id).get
    parse(content).extract[Ssd]
  }

  test("API version number should be 1.0") {
    assert(APIVersion === "v1.0")
  }

  test(s"POSTing to /$APIVersion/ssd should create an SSD") (new TestServer {
    try {
      val (request, ssd) = createSsd(DatasetDocument, SsdDocument).get

      ssd should have (
        'name (request.name.get),
        'ontologies (request.ontologies.get),
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

  test(s"Creating SSD with invalid request should get bad-request response") (new TestServer {
    try {
      val request = parse(SsdDocument).extract[SsdRequest]
      val (status, _) = requestSsdCreation(request).get

      status should be (BadRequest)
    } finally {
      deleteAllSsds
      deleteAllDatasets
      assertClose()
    }
  })

  test(s"DELETEing /$APIVersion/ssd/:id should delete an SSD") (new TestServer {
    try {
      val createdSsd = createSsd(DatasetDocument, SsdDocument).get._2
      val (status, _) = requestSsdDeletion(createdSsd.id).get
      val ssds = listSsds.get

      status should be (Ok)
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
        name = Some(updatedName),
        ontologies = None,
        semanticModel = None,
        mappings = None)
      val updatedSsd = updateSsd(request, createdSsd.id).get

      updatedSsd should have (
        'id (createdSsd.id),
        'name (updatedName),
        'attributes (createdSsd.attributes),
        'ontologies (createdSsd.ontologies),
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

  test(s"Updating SSD with nonexistent ID should get not-found response") (new TestServer {
    try {
      val (status, _) = requestSsdUpdate(SsdRequest(None, None, None, None), 0).get

      status should be (NotFound)
    } finally {
      deleteAllSsds
      deleteAllDatasets
      assertClose()
    }
  })

  test(s"Updating SSD with invalid request should get bad-request response") (new TestServer {
    try {
      val (_, ssd) = createSsd(DatasetDocument, SsdDocument).get
      val (status, _) = requestSsdUpdate(parse(SsdDocument).extract[SsdRequest], ssd.id).get

      status should be (BadRequest)
    } finally {
      deleteAllSsds
      deleteAllDatasets
      assertClose()
    }
  })

  //==============================================================================
  lazy val OctopusHelp = new OctopusAPISpec

  test("POST ssd responds BadRequest since mappings for attributes are not unique") (new TestServer {
    try {
      OctopusHelp.setUp()

      val inc = JObject(
        List(("name",JString("getCities.csv")),
          ("ontologies",JArray(List(JInt(1)))),
          ("semanticModel", JObject(
            List(
              ("nodes",JArray(List(
                JObject(List(("id",JInt(0)), ("label",JString("State")), ("type",JString("ClassNode")), ("status",JString("ForcedByUser")), ("prefix",JString("http://www.semanticweb.org/serene/report_example_ontology#")))),
                JObject(List(("id",JInt(1)), ("label",JString("State.name")), ("type",JString("DataNode")), ("status",JString("ForcedByUser")))),
                JObject(List(("id",JInt(3)), ("label",JString("City.name")), ("type",JString("DataNode")), ("status",JString("ForcedByUser")))),
                JObject(List(("id",JInt(2)), ("label",JString("City")), ("type",JString("ClassNode")), ("status",JString("ForcedByUser")), ("prefix",JString("http://www.semanticweb.org/serene/report_example_ontology#"))))))),
              ("links",JArray(List(
                JObject(List(("id",JInt(1)), ("source",JInt(0)), ("target",JInt(1)), ("label",JString("name")), ("type",JString("DataPropertyLink")), ("status",JString("ForcedByUser")), ("prefix",JString("http://www.semanticweb.org/serene/report_example_ontology#")))),
                JObject(List(("id",JInt(0)), ("source",JInt(2)), ("target",JInt(0)), ("label",JString("isPartOf")), ("type",JString("ObjectPropertyLink")), ("status",JString("ForcedByUser")), ("prefix",JString("http://www.semanticweb.org/serene/report_example_ontology#")))),
                JObject(List(("id",JInt(2)), ("source",JInt(2)), ("target",JInt(3)), ("label",JString("name")), ("type",JString("DataPropertyLink")), ("status",JString("ForcedByUser")), ("prefix",JString("http://www.semanticweb.org/serene/report_example_ontology#")))))))))),
          ("mappings",JArray(List(
            JObject(List(("attribute",JInt(1997319549)), ("node",JInt(1)))),
            JObject(List(("attribute",JInt(1160349990)), ("node",JInt(0))))
            ,JObject(List(("attribute",JInt(1160349990)), ("node",JInt(3))))
          )))))

      val req = OctopusHelp.postRequest(json = inc, url = s"/$APIVersion/ssd")
      // send the request and make sure it executes
      val resp = Await.result(client(req))

      assert(resp.status === Status.BadRequest)
      // FIXME: error message is bad
      assert(resp.contentString.nonEmpty)

    } finally {
      deleteAllSsds
      deleteAllDatasets
      assertClose()
    }
  })

  test("POST ssd responds BadRequest since nodes are absent in semantic model") (new TestServer {
    try {
      OctopusHelp.setUp()

      val inc = JObject(
        List(("name",JString("getCities.csv")),
          ("ontologies",JArray(List(JInt(1)))),
          ("semanticModel", JObject(
            List(
              ("links",JArray(List(
                JObject(List(("id",JInt(1)), ("source",JInt(0)), ("target",JInt(1)), ("label",JString("name")), ("type",JString("DataPropertyLink")), ("status",JString("ForcedByUser")), ("prefix",JString("http://www.semanticweb.org/serene/report_example_ontology#")))),
                JObject(List(("id",JInt(0)), ("source",JInt(2)), ("target",JInt(0)), ("label",JString("isPartOf")), ("type",JString("ObjectPropertyLink")), ("status",JString("ForcedByUser")), ("prefix",JString("http://www.semanticweb.org/serene/report_example_ontology#")))),
                JObject(List(("id",JInt(2)), ("source",JInt(2)), ("target",JInt(3)), ("label",JString("name")), ("type",JString("DataPropertyLink")), ("status",JString("ForcedByUser")), ("prefix",JString("http://www.semanticweb.org/serene/report_example_ontology#")))))))))),
          ("mappings",JArray(List(
            JObject(List(("attribute",JInt(1997319549)), ("node",JInt(1)))),
            JObject(List(("attribute",JInt(1160349990)), ("node",JInt(0))))
          )))))

      val req = OctopusHelp.postRequest(json = inc, url = s"/$APIVersion/ssd")
      // send the request and make sure it executes
      val resp = Await.result(client(req))

      assert(resp.status === Status.BadRequest)
      // FIXME: error message is bad
      assert(resp.contentString.nonEmpty)

    } finally {
      deleteAllSsds
      deleteAllDatasets
      assertClose()
    }
  })

  test("POST cities ssd responds Ok") (new TestServer {
    try {
      OctopusHelp.setUp()

      val inc = JObject(
        List(("name",JString("getCities.csv")),
          ("ontologies",JArray(List(JInt(1)))),
          ("semanticModel", JObject(
            List(
              ("nodes",JArray(List(
                JObject(List(("id",JInt(0)), ("label",JString("State")), ("type",JString("ClassNode")), ("status",JString("ForcedByUser")), ("prefix",JString("http://www.semanticweb.org/serene/report_example_ontology#")))),
                JObject(List(("id",JInt(1)), ("label",JString("State.name")), ("type",JString("DataNode")), ("status",JString("ForcedByUser")))),
                JObject(List(("id",JInt(3)), ("label",JString("City.name")), ("type",JString("DataNode")), ("status",JString("ForcedByUser")))),
                JObject(List(("id",JInt(2)), ("label",JString("City")), ("type",JString("ClassNode")), ("status",JString("ForcedByUser")), ("prefix",JString("http://www.semanticweb.org/serene/report_example_ontology#"))))))),
              ("links",JArray(List(
                JObject(List(("id",JInt(1)), ("source",JInt(0)), ("target",JInt(1)), ("label",JString("name")), ("type",JString("DataPropertyLink")), ("status",JString("ForcedByUser")), ("prefix",JString("http://www.semanticweb.org/serene/report_example_ontology#")))),
                JObject(List(("id",JInt(0)), ("source",JInt(2)), ("target",JInt(0)), ("label",JString("isPartOf")), ("type",JString("ObjectPropertyLink")), ("status",JString("ForcedByUser")), ("prefix",JString("http://www.semanticweb.org/serene/report_example_ontology#")))),
                JObject(List(("id",JInt(2)), ("source",JInt(2)), ("target",JInt(3)), ("label",JString("name")), ("type",JString("DataPropertyLink")), ("status",JString("ForcedByUser")), ("prefix",JString("http://www.semanticweb.org/serene/report_example_ontology#")))))))))),
          ("mappings",JArray(List(
            JObject(List(("attribute",JInt(1997319549)), ("node",JInt(1)))),
            JObject(List(("attribute",JInt(1160349990)), ("node",JInt(0))))
          )))))

      val req = OctopusHelp.postRequest(json = inc, url = s"/$APIVersion/ssd")
      // send the request and make sure it executes
      val resp = Await.result(client(req))

      assert(resp.status === Status.Ok)
      assert(resp.contentString.nonEmpty)
      assert(Try{parse(resp.contentString).extract[Ssd]}.isSuccess)

    } finally {
      deleteAllSsds
      deleteAllDatasets
      assertClose()
    }
  })

  test("POST cities ssd responds responds BadRequest since target key is missing in links") (new TestServer {
    try {
      OctopusHelp.setUp()

      val inc = JObject(
        List(("name",JString("getCities.csv")),
          ("ontologies",JArray(List(JInt(1)))),
          ("semanticModel", JObject(
            List(
              ("nodes",JArray(List(
                JObject(List(("id",JInt(0)), ("label",JString("State")), ("type",JString("ClassNode")), ("status",JString("ForcedByUser")), ("prefix",JString("http://www.semanticweb.org/serene/report_example_ontology#")))),
                JObject(List(("id",JInt(1)), ("label",JString("State.name")), ("type",JString("DataNode")), ("status",JString("ForcedByUser")))),
                JObject(List(("id",JInt(3)), ("label",JString("City.name")), ("type",JString("DataNode")), ("status",JString("ForcedByUser")))),
                JObject(List(("id",JInt(2)), ("label",JString("City")), ("type",JString("ClassNode")), ("status",JString("ForcedByUser")), ("prefix",JString("http://www.semanticweb.org/serene/report_example_ontology#"))))))),
              ("links",JArray(List(
                JObject(List(("id",JInt(1)), ("source",JInt(0)),  ("label",JString("name")), ("type",JString("DataPropertyLink")), ("status",JString("ForcedByUser")), ("prefix",JString("http://www.semanticweb.org/serene/report_example_ontology#")))),
                JObject(List(("id",JInt(0)), ("source",JInt(2)), ("target",JInt(0)), ("label",JString("isPartOf")), ("type",JString("ObjectPropertyLink")), ("status",JString("ForcedByUser")), ("prefix",JString("http://www.semanticweb.org/serene/report_example_ontology#")))),
                JObject(List(("id",JInt(2)), ("source",JInt(2)), ("target",JInt(3)), ("label",JString("name")), ("type",JString("DataPropertyLink")), ("status",JString("ForcedByUser")), ("prefix",JString("http://www.semanticweb.org/serene/report_example_ontology#")))))))))),
          ("mappings",JArray(List(
            JObject(List(("attribute",JInt(1997319549)), ("node",JInt(1)))),
            JObject(List(("attribute",JInt(1160349990)), ("node",JInt(0))))
          )))))

      val req = OctopusHelp.postRequest(json = inc, url = s"/$APIVersion/ssd")
      // send the request and make sure it executes
      val resp = Await.result(client(req))

      assert(resp.status === Status.BadRequest)
      assert(resp.contentString.nonEmpty)

    } finally {
      deleteAllSsds
      deleteAllDatasets
      assertClose()
    }
  })

  test("POST cities ssd responds responds BadRequest since target id is wrong in links") (new TestServer {
    try {
      OctopusHelp.setUp()

      val inc = JObject(
        List(("name",JString("getCities.csv")),
          ("ontologies",JArray(List(JInt(1)))),
          ("semanticModel", JObject(
            List(
              ("nodes",JArray(List(
                JObject(List(("id",JInt(0)), ("label",JString("State")), ("type",JString("ClassNode")), ("status",JString("ForcedByUser")), ("prefix",JString("http://www.semanticweb.org/serene/report_example_ontology#")))),
                JObject(List(("id",JInt(1)), ("label",JString("State.name")), ("type",JString("DataNode")), ("status",JString("ForcedByUser")))),
                JObject(List(("id",JInt(3)), ("label",JString("City.name")), ("type",JString("DataNode")), ("status",JString("ForcedByUser")))),
                JObject(List(("id",JInt(2)), ("label",JString("City")), ("type",JString("ClassNode")), ("status",JString("ForcedByUser")), ("prefix",JString("http://www.semanticweb.org/serene/report_example_ontology#"))))))),
              ("links",JArray(List(
                JObject(List(("id",JInt(1)), ("source",JInt(0)), ("target",JInt(1243)), ("label",JString("name")), ("type",JString("DataPropertyLink")), ("status",JString("ForcedByUser")), ("prefix",JString("http://www.semanticweb.org/serene/report_example_ontology#")))),
                JObject(List(("id",JInt(0)), ("source",JInt(2)), ("target",JInt(0)), ("label",JString("isPartOf")), ("type",JString("ObjectPropertyLink")), ("status",JString("ForcedByUser")), ("prefix",JString("http://www.semanticweb.org/serene/report_example_ontology#")))),
                JObject(List(("id",JInt(2)), ("source",JInt(2)), ("target",JInt(3)), ("label",JString("name")), ("type",JString("DataPropertyLink")), ("status",JString("ForcedByUser")), ("prefix",JString("http://www.semanticweb.org/serene/report_example_ontology#")))))))))),
          ("mappings",JArray(List(
            JObject(List(("attribute",JInt(1997319549)), ("node",JInt(1)))),
            JObject(List(("attribute",JInt(1160349990)), ("node",JInt(0))))
          )))))

      val req = OctopusHelp.postRequest(json = inc, url = s"/$APIVersion/ssd")
      // send the request and make sure it executes
      val resp = Await.result(client(req))

      assert(resp.status === Status.BadRequest)
      assert(resp.contentString.nonEmpty)

    } finally {
      deleteAllSsds
      deleteAllDatasets
      assertClose()
    }
  })

  test("POST cities ssd responds responds BadRequest since label is missing in nodes") (new TestServer {
    try {
      OctopusHelp.setUp()

      val inc = JObject(
        List(("name",JString("getCities.csv")),
          ("ontologies",JArray(List(JInt(1)))),
          ("semanticModel", JObject(
            List(
              ("nodes",JArray(List(
                JObject(List(("id",JInt(0)), ("type",JString("ClassNode")), ("status",JString("ForcedByUser")), ("prefix",JString("http://www.semanticweb.org/serene/report_example_ontology#")))),
                JObject(List(("id",JInt(1)), ("label",JString("State.name")), ("type",JString("DataNode")), ("status",JString("ForcedByUser")))),
                JObject(List(("id",JInt(3)), ("label",JString("City.name")), ("type",JString("DataNode")), ("status",JString("ForcedByUser")))),
                JObject(List(("id",JInt(2)), ("label",JString("City")), ("type",JString("ClassNode")), ("status",JString("ForcedByUser")), ("prefix",JString("http://www.semanticweb.org/serene/report_example_ontology#"))))))),
              ("links",JArray(List(
                JObject(List(("id",JInt(1)), ("source",JInt(0)), ("target",JInt(1)), ("label",JString("name")), ("type",JString("DataPropertyLink")), ("status",JString("ForcedByUser")), ("prefix",JString("http://www.semanticweb.org/serene/report_example_ontology#")))),
                JObject(List(("id",JInt(0)), ("source",JInt(2)), ("target",JInt(0)), ("label",JString("isPartOf")), ("type",JString("ObjectPropertyLink")), ("status",JString("ForcedByUser")), ("prefix",JString("http://www.semanticweb.org/serene/report_example_ontology#")))),
                JObject(List(("id",JInt(2)), ("source",JInt(2)), ("target",JInt(3)), ("label",JString("name")), ("type",JString("DataPropertyLink")), ("status",JString("ForcedByUser")), ("prefix",JString("http://www.semanticweb.org/serene/report_example_ontology#")))))))))),
          ("mappings",JArray(List(
            JObject(List(("attribute",JInt(1997319549)), ("node",JInt(1)))),
            JObject(List(("attribute",JInt(1160349990)), ("node",JInt(0))))
          )))))

      val req = OctopusHelp.postRequest(json = inc, url = s"/$APIVersion/ssd")
      // send the request and make sure it executes
      val resp = Await.result(client(req))

      assert(resp.status === Status.BadRequest)
      assert(resp.contentString.nonEmpty)

    } finally {
      deleteAllSsds
      deleteAllDatasets
      assertClose()
    }
  })
}
