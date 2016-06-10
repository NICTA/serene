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
package au.csiro.data61.matcher


import java.io.File

import au.csiro.data61.matcher.types.Feature.{NUM_ALPHA, NUM_CHARS, IS_ALPHA}
import au.csiro.data61.matcher.types.ModelTypes.{Model, ModelID}
import au.csiro.data61.matcher.types.MatcherJsonFormats
import com.twitter.finagle.http.RequestBuilder
import com.twitter.finagle.http._

import com.twitter.io.Buf
import com.twitter.util.Await
import org.apache.commons.io.FileUtils
import org.junit.runner.RunWith

import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.scalatest.junit.JUnitRunner
import api._

import language.postfixOps

import scala.util.{Failure, Success, Try, Random}

import org.json4s._
import org.json4s.jackson.JsonMethods._

/**
 * Tests for the Model REST endpoint API
 */
@RunWith(classOf[JUnitRunner])
class ModelRestAPISpec extends FunSuite with MatcherJsonFormats with BeforeAndAfterEach {

  import ModelRestAPI._

  override def beforeEach() {
    FileUtils.deleteDirectory(new File(Config.DatasetStorageDir))
  }

  override def afterEach() {
    FileUtils.deleteDirectory(new File(Config.DatasetStorageDir))
  }
  /**
   * Posts a request to build a model, then returns the Model object it created
   * wrapped in a Try.
   *
   * @param server Test server for this request
   * @param labels The model request object
   * @return Model that was constructed
   */
  def postAndReturn(server: TestServer,
                    labels: List[String],
                    description: Option[String] = None): Try[Model] = {

    Try {

      val req = RequestBuilder()
        .url(server.fullUrl(s"/$APIVersion/model"))
        .addHeader("Content-Type", "application/json")
        .buildPost(Buf.Utf8(
          s"""
             |  {
             |    "description": "${description.getOrElse("unknown")}",
             |    "modelType": "randomForest",
             |    "labels": [${labels.map(x => s""""$x"""").mkString(",")}],
             |    "features": ["isAlpha", "numChars", "numAlpha"],
             |    "training": {"n": 10},
             |    "costMatrix": [[1,0,0], [0,1,0], [0,0,1]],
             |    "resamplingStrategy": "ResampleToMean"
             |  }
           """.stripMargin))

      val response = Await.result(server.client(req))

      parse(response.contentString).extract[Model]
    }
  }

  test("version number is 1.0") {
    assert(APIVersion === "v1.0")
  }

  test("GET /v1.0/model responds Ok(200)") (new TestServer {
    try {
      val response = get(s"/$APIVersion/model")
      assert(response.contentType === Some(JsonHeader))
      assert(response.status === Status.Ok)
      assert(!response.contentString.isEmpty)
      assert(Try { parse(response.contentString).extract[List[ModelID]] }.isSuccess)
    } finally {
      assertClose()
    }
  })

  test("POST /v1.0/model responds Ok(200)") (new TestServer {
    try {

      val testStr = Random.alphanumeric take 10 mkString

      val request = RequestBuilder()
        .url(fullUrl(s"/$APIVersion/model"))
        .addHeader("Content-Type", "application/json")
        .buildPost(Buf.Utf8(
          s"""
             |  {
             |    "description": "$testStr",
             |    "modelType": "randomForest",
             |    "labels": ["name", "address", "phone"],
             |    "features": ["isAlpha", "numChars", "numAlpha"],
             |    "training": {"n": 10},
             |    "costMatrix": [[1,0,0], [0,1,0], [0,0,1]],
             |    "resamplingStrategy": "ResampleToMean"
             |  }
           """.stripMargin))

      val response = Await.result(client(request))

      assert(response.contentType === Some(JsonHeader))
      assert(response.status === Status.Ok)
      assert(!response.contentString.isEmpty)
      val model = parse(response.contentString).extract[Model]

      assert(model.description === testStr)

    } finally {
      assertClose()
    }
  })

  test("POST /v1.0/model only labels and features required") (new TestServer {
    try {

      val request = RequestBuilder()
        .url(fullUrl(s"/$APIVersion/model"))
        .addHeader("Content-Type", "application/json")
        .buildPost(Buf.Utf8(
          s"""
             |  {
             |    "labels": ["name", "address", "phone"],
             |    "features": ["isAlpha", "numChars", "numAlpha"]
             |  }
           """.stripMargin))

      val response = Await.result(client(request))

      assert(response.contentType === Some(JsonHeader))
      assert(response.status === Status.Ok)
      assert(!response.contentString.isEmpty)
      val model = parse(response.contentString).extract[Model]

      assert(model.labels === List("name", "address", "phone"))
      assert(model.features === List(IS_ALPHA, NUM_CHARS, NUM_ALPHA))

    } finally {
      assertClose()
    }
  })

  test("POST /v1.0/model fails if labels not present BadRequest(400)") (new TestServer {
    try {

      val request = RequestBuilder()
        .url(fullUrl(s"/$APIVersion/model"))
        .addHeader("Content-Type", "application/json")
        .buildPost(Buf.Utf8(
          s"""
             |  {
             |    "description": "hello"
             |  }
           """.stripMargin))

      val response = Await.result(client(request))

      assert(response.contentType === Some(JsonHeader))
      assert(response.status === Status.BadRequest)
      assert(!response.contentString.isEmpty)

    } finally {
      assertClose()
    }
  })

  test("POST /v1.0/model appears in model list") (new TestServer {
    try {
      val labels = List("name", "addr","asdf")

      postAndReturn(this, labels) match {
        case Success(model) =>

          // build a request to modify the model...
          val response = get(s"/$APIVersion/model")
          assert(response.contentType === Some(JsonHeader))
          assert(response.status === Status.Ok)
          assert(!response.contentString.isEmpty)

          // ensure that the object appears in the master list...
          val datasets = parse(response.contentString).extract[List[Int]]
          assert(datasets.contains(model.id))

        case Failure(err) =>
          throw new Exception("Failed to create test resource")
      }
    } finally {
      assertClose()
    }
  })

  test("GET /v1.0/model/id responds Ok(200)") (new TestServer {
    try {
      val TestStr = Random.alphanumeric take 10 mkString
      val TestLabels = List("name", "addr","asdf")

      postAndReturn(this, TestLabels, Some(TestStr)) match {
        case Success(model) =>

          // build a request to modify the model...
          val response = get(s"/$APIVersion/model/${model.id}")
          assert(response.contentType === Some(JsonHeader))
          assert(response.status === Status.Ok)
          assert(!response.contentString.isEmpty)

          // ensure that the data is correct...
          val returnedModel = parse(response.contentString).extract[Model]

          assert(returnedModel.description  === model.description)
          assert(returnedModel.description  === TestStr)
          assert(returnedModel.dateCreated  === model.dateCreated)
          assert(returnedModel.dateModified === model.dateModified)

        case Failure(err) =>
          throw new Exception("Failed to create test resource")
      }

    } finally {
      assertClose()
    }
  })

  test("GET /v1.0/model/id missing id returns NotFound(404)") (new TestServer {
    try {
      // Attempt to grab a dataset at zero. This should be
      // converted to an int successfully but cannot be created
      // by the id gen.
      val response = get(s"/$APIVersion/model/0")
      assert(response.contentType === Some(JsonHeader))
      assert(response.status === Status.NotFound)
      assert(!response.contentString.isEmpty)
    } finally {
      assertClose()
    }
  })

  test("GET /v1.0/model/id non-int returns empty NotFound(404)") (new TestServer {
    try {
      // Attempt to grab a model at 'asdf'. This should be
      // converted to an int successfully but cannot be created
      // by the id gen.
      val response = get(s"/$APIVersion/model/asdf")
      assert(response.status === Status.NotFound)
      assert(response.contentString.isEmpty)
    } finally {
      assertClose()
    }
  })

//  test("POST /v1.0/model/id responds Ok(200)") (new TestServer {
//    try {
//      val TypeMap = """{"w":"x", "y":"z"}"""
//      val TestStr = Random.alphanumeric take 10 mkString
//      val PauseTime = 2000
//
//      postAndReturn(this, Resource, "{}", "") match {
//        case Success(ds) =>
//          // wait for the clock to tick
//          Thread.sleep(PauseTime)
//
//          // build a request to modify the dataset...
//          val request = RequestBuilder()
//            .url(fullUrl(s"/$APIVersion/dataset/${ds.id}"))
//            .addFormElement("description" -> TestStr)
//            .addFormElement("typeMap" -> TypeMap)
//            .buildFormPost(multipart = false)
//
//          // send the request and make sure it executes
//          val response = Await.result(client(request))
//          assert(response.contentType === Some(JsonHeader))
//          assert(response.status === Status.Ok)
//          assert(!response.contentString.isEmpty)
//
//          // ensure that the data is correct...
//          val patchDS = parse(response.contentString).extract[DataSet]
//          assert(patchDS.description === TestStr)
//          assert(patchDS.dateCreated !== patchDS.dateModified)
//          assert(patchDS.typeMap.get("w") === Some("x"))
//          assert(patchDS.typeMap.get("y") === Some("z"))
//
//        case Failure(err) =>
//          throw new Exception("Failed to create test resource")
//      }
//    } finally {
//      assertClose()
//    }
//  })

//  test("DELETE /v1.0/dataset/id responds Ok(200)") (new TestServer {
//    try {
//      val TypeMap = """{"w":"x", "y":"z"}"""
//      val TestStr = Random.alphanumeric take 10 mkString
//
//      postAndReturn(this, Resource, TypeMap, TestStr) match {
//        case Success(ds) =>
//
//          // build a request to modify the dataset...
//          val resource = s"/$APIVersion/dataset/${ds.id}"
//
//          val response = delete(resource)
//          assert(response.contentType === Some(JsonHeader))
//          assert(response.status === Status.Ok)
//          assert(!response.contentString.isEmpty)
//
//          // there should be nothing there, and the response
//          // should say so.
//          val noResource = get(resource)
//          assert(noResource.contentType === Some(JsonHeader))
//          assert(noResource.status === Status.NotFound)
//          assert(!noResource.contentString.isEmpty)
//
//        case Failure(err) =>
//          throw new Exception("Failed to create test resource")
//      }
//    } finally {
//      assertClose()
//    }
//  })

}
