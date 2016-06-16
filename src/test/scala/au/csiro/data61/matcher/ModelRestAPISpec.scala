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

import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._
/**
 * Tests for the Model REST endpoint API
 */
@RunWith(classOf[JUnitRunner])
class ModelRestAPISpec extends FunSuite with MatcherJsonFormats with BeforeAndAfterEach {

  import ModelRestAPI._

  val DefaultLabelCount = 4

  override def beforeEach() {
    FileUtils.deleteDirectory(new File(Config.ModelStorageDir))
  }

  override def afterEach() {
    FileUtils.deleteDirectory(new File(Config.ModelStorageDir))
  }

  def randomString: String = Random.alphanumeric take 10 mkString

  /**
   * Builds a standard POST request object from a json object.
   *
   * @param json
   * @param url
   * @return
   */
  def postRequest(json: JObject, url: String = s"/$APIVersion/model")(implicit s: TestServer): Request = {
    RequestBuilder()
      .url(s.fullUrl(url))
      .addHeader("Content-Type", "application/json")
      .buildPost(Buf.Utf8(compact(render(json))))
  }

  /**
   * Posts a request to build a model, then returns the Model object it created
   * wrapped in a Try.
   *
   * @param labels The model request object
   * @return Model that was constructed
   */
  def postAndReturn(labels: List[String],
                    description: Option[String] = None)(implicit s: TestServer): Try[Model] = {

    Try {

      val json =
        ("description" -> description.getOrElse("unknown")) ~
          ("modelType" -> "randomForest") ~
          ("labels" -> labels) ~
          ("features" -> Seq("isAlpha", "numChars", "numAlpha") ) ~
          ("costMatrix" -> JArray(List(JArray(List(1,0,0)), JArray(List(0,1,0)), JArray(List(0,0,1))))) ~
          ("resamplingStrategy" -> "ResampleToMean")

      val req = postRequest(json)

      val response = Await.result(s.client(req))

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
      val LabelLength = 4
      val TestStr = randomString
      val TestLabels = List.fill(LabelLength)(randomString)

      val json =
        ("description" -> TestStr) ~
          ("modelType" -> "randomForest") ~
          ("labels" -> TestLabels) ~
          ("features" -> Seq("isAlpha", "numChars", "numAlpha") ) ~
          ("costMatrix" -> JArray(List(JArray(List(1,0,0)), JArray(List(0,1,0)), JArray(List(0,0,1))))) ~
          ("resamplingStrategy" -> "ResampleToMean")

      val request = postRequest(json)

      val response = Await.result(client(request))

      assert(response.contentType === Some(JsonHeader))
      assert(response.status === Status.Ok)
      assert(!response.contentString.isEmpty)
      val model = parse(response.contentString).extract[Model]

      assert(model.description === TestStr)
      assert(model.labels === TestLabels)

    } finally {
      assertClose()
    }
  })

  test("POST /v1.0/model only labels and features required") (new TestServer {
    try {

      val LabelLength = 4
      val TestLabels = List.fill(LabelLength)(randomString)

      val json =
        ("labels" -> TestLabels) ~
          ("features" -> Seq("isAlpha", "numChars", "numAlpha"))

      val request = postRequest(json)

      val response = Await.result(client(request))

      assert(response.contentType === Some(JsonHeader))
      assert(response.status === Status.Ok)
      assert(!response.contentString.isEmpty)
      val model = parse(response.contentString).extract[Model]

      assert(model.labels === TestLabels)
      assert(model.features === List(IS_ALPHA, NUM_CHARS, NUM_ALPHA))

    } finally {
      assertClose()
    }
  })

  test("POST /v1.0/model fails if labels not present BadRequest(400)") (new TestServer {
    try {

      // some request without labels...
      val json = "description" -> randomString

      val request = postRequest(json)

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
      val LabelLength = 4
      val TestLabels = List.fill(LabelLength)(randomString)

      postAndReturn(TestLabels) match {
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
      val LabelLength = 4
      val TestStr = randomString
      val TestLabels = List.fill(LabelLength)(randomString)

      postAndReturn(TestLabels, Some(TestStr)) match {
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

  test("POST /v1.0/model/id responds Ok(200)") (new TestServer {
    try {
      val LabelLength = 4
      val TestStr = randomString
      val TestLabels = List.fill(LabelLength)(randomString)
      val PauseTime = 2000

      val NewDescription = randomString
      val NewLabels = List.fill(LabelLength)(randomString)

      postAndReturn(TestLabels, Some(TestStr)) match {
        case Success(ds) =>
          // wait for the clock to tick for the dateModified
          Thread.sleep(PauseTime)

          val json =
            ("description" -> NewDescription) ~
              ("modelType" -> "randomForest") ~
              ("labels" -> NewLabels) ~
              ("features" -> Seq("isAlpha", "numChars", "numAlpha") ) ~
              ("costMatrix" -> "[[1,0,0], [0,1,0], [0,0,1]]") ~
              ("resamplingStrategy" -> "ResampleToMean")

          val request = postRequest(json, s"/$APIVersion/model/${ds.id}")

          // send the request and make sure it executes
          val response = Await.result(client(request))
          assert(response.contentType === Some(JsonHeader))
          assert(response.status === Status.Ok)
          assert(!response.contentString.isEmpty)

          // ensure that the data is correct...
          val patchModel = parse(response.contentString).extract[Model]
          assert(patchModel.description === NewDescription)
          assert(patchModel.labels === NewLabels)
          assert(patchModel.dateCreated !== patchModel.dateModified)

        case Failure(err) =>
          throw new Exception("Failed to create test resource")
      }
    } finally {
      assertClose()
    }
  })

  test("DELETE /v1.0/model/id responds Ok(200)") (new TestServer {
    try {
      val TestStr = randomString
      val NumLabels = 4
      val TestLabels = List.fill(NumLabels)(randomString)

      postAndReturn(TestLabels, Some(TestStr)) match {
        case Success(model) =>

          // build a request to modify the dataset...
          val resource = s"/$APIVersion/model/${model.id}"

          val response = delete(resource)
          assert(response.contentType === Some(JsonHeader))
          assert(response.status === Status.Ok)
          assert(!response.contentString.isEmpty)

          // there should be nothing there, and the response
          // should say so.
          val noResource = get(resource)
          assert(noResource.contentType === Some(JsonHeader))
          assert(noResource.status === Status.NotFound)
          assert(!noResource.contentString.isEmpty)

        case Failure(err) =>
          throw new Exception("Failed to create test resource")
      }
    } finally {
      assertClose()
    }
  })

}
