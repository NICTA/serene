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

import au.csiro.data61.matcher.types.{MatcherJsonFormats, DataSet}
import com.twitter.finagle.http.RequestBuilder
import com.twitter.finagle.http._

import com.twitter.io.Reader
import com.twitter.util.Await
import org.junit.runner.RunWith

import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import api._

import language.postfixOps

import scala.util.{Failure, Success, Try, Random}

import org.json4s._
import org.json4s.jackson.JsonMethods._

/**
 * Tests for the Dataset REST endpoint API
 */
@RunWith(classOf[JUnitRunner])
class DatasetRestAPISpec extends FunSuite with MatcherJsonFormats {

  import DatasetRestAPI._

  val Resource = getClass.getResource("/medium.csv").getPath

  /**
   * Posts a request to build a dataset, then returns the DataSet object it created
   * wrapped in a Try.
   *
   * @param file The location of the csv resource
   * @param typeMap The json string of the string->string typemap
   * @param description Description line to add to the file.
   * @return DataSet that was constructed
   */
  def postAndReturn(server: TestServer, file: String, typeMap: String, description: String): Try[DataSet] = {

    Try {
      val content = Await.result(Reader.readAll(Reader.fromFile(new File(file))))

      val request = RequestBuilder().url(server.fullUrl(s"/$APIVersion/dataset"))
        .addFormElement("description" -> description)
        .addFormElement("typeMap" -> typeMap)
        .add(FileElement("file", content, None, Some(typeMap)))
        .buildFormPost(multipart = true)

      val response = Await.result(server.client(request))

      parse(response.contentString).extract[DataSet]
    }
  }

  test("version number is 1.0") {
    assert(APIVersion === "v1.0")
  }

  test("GET /v1.0/dataset responds Ok(200)") (new TestServer {
    try {
      val response = get(s"/$APIVersion/dataset")
      assert(response.contentType === Some(JsonHeader))
      assert(response.status === Status.Ok)
      assert(!response.contentString.isEmpty)
    } finally {
      assertClose()
    }
  })

  test("POST /v1.0/dataset responds Ok(200)") (new TestServer {
    try {
      val TypeMap = """{"a":"b", "c":"d"}"""
      val content = Await.result(Reader.readAll(Reader.fromFile(new File(Resource))))
      val testStr = Random.alphanumeric take 10 mkString
      val fileName = Random.alphanumeric take 10 mkString

      val request = RequestBuilder()
        .url(fullUrl(s"/$APIVersion/dataset"))
        .addFormElement("description" -> testStr)
        .addFormElement("typeMap" -> TypeMap)
        .add(FileElement("file", content, None, Some(fileName)))
        .buildFormPost(multipart = true)

      val response = Await.result(client(request))

      assert(response.contentType === Some(JsonHeader))
      assert(response.status === Status.Ok)
      assert(!response.contentString.isEmpty)

      val ds = parse(response.contentString).extract[DataSet]

      assert(ds.description === testStr)
      assert(ds.filename === fileName)
      assert(ds.dateCreated === ds.dateModified)
      assert(ds.typeMap.get("a") === Some("b"))
      assert(ds.typeMap.get("c") === Some("d"))

    } finally {
      assertClose()
    }
  })

  test("POST /v1.0/dataset without file returns BadRequest(400)") (new TestServer {
    try {

      val request = RequestBuilder()
        .url(fullUrl(s"/$APIVersion/dataset"))
        .addFormElement("description" -> "")
        .addFormElement("typeMap" -> "{}")
        .buildFormPost(multipart = true)

      val response = Await.result(client(request))

      assert(response.contentType === Some(JsonHeader))
      assert(response.status === Status.BadRequest)
      assert(!response.contentString.isEmpty)

    } finally {
      assertClose()
    }
  })

  test("POST /v1.0/dataset appears in dataset list") (new TestServer {
    try {
      postAndReturn(this, Resource, "{}", "") match {
        case Success(ds) =>

          // build a request to modify the dataset...
          val response = get(s"/$APIVersion/dataset")
          assert(response.contentType === Some(JsonHeader))
          assert(response.status === Status.Ok)
          assert(!response.contentString.isEmpty)

          // ensure that the object appears in the master list...
          val datasets = parse(response.contentString).extract[List[Int]]
          assert(datasets.contains(ds.id))

        case Failure(err) =>
          throw new Exception("Failed to create test resource")
      }
    } finally {
      assertClose()
    }
  })

  test("GET /v1.0/dataset/id responds Ok(200)") (new TestServer {
    try {
      val TypeMap = """{"w":"x", "y":"z"}"""
      val TestStr = Random.alphanumeric take 10 mkString

      postAndReturn(this, Resource, TypeMap, TestStr) match {
        case Success(ds) =>

          // build a request to modify the dataset...
          val response = get(s"/$APIVersion/dataset/${ds.id}")
          assert(response.contentType === Some(JsonHeader))
          assert(response.status === Status.Ok)
          assert(!response.contentString.isEmpty)

          // ensure that the data is correct...
          val returnedDS = parse(response.contentString).extract[DataSet]
          assert(returnedDS.description === ds.description)
          assert(returnedDS.description === TestStr)
          assert(returnedDS.dateCreated === ds.dateCreated)
          assert(returnedDS.dateModified === ds.dateModified)
          assert(returnedDS.typeMap === ds.typeMap)

        case Failure(err) =>
          throw new Exception("Failed to create test resource")
      }
    } finally {
      assertClose()
    }
  })

  test("GET /v1.0/dataset/id?samples=4 responds Ok(200)") (new TestServer {
    try {
      val TypeMap = """{"w":"x", "y":"z"}"""
      val TestStr = Random.alphanumeric take 10 mkString
      val SampleLength = 4

      postAndReturn(this, Resource, TypeMap, TestStr) match {
        case Success(ds) =>

          // build a request to modify the dataset...
          val response = get(s"/$APIVersion/dataset/${ds.id}?samples=$SampleLength")
          assert(response.contentType === Some(JsonHeader))
          assert(response.status === Status.Ok)
          assert(!response.contentString.isEmpty)

          // ensure that the data is correct...
          val returnedDS = parse(response.contentString).extract[DataSet]
          assert(returnedDS.description === ds.description)
          assert(returnedDS.description === TestStr)
          assert(returnedDS.columns.head.sample.length == SampleLength)
          assert(returnedDS.dateCreated === ds.dateCreated)
          assert(returnedDS.dateModified === ds.dateModified)
          assert(returnedDS.typeMap === ds.typeMap)

        case Failure(err) =>
          throw new Exception("Failed to create test resource")
      }
    } finally {
      assertClose()
    }
  })

  test("GET /v1.0/dataset/id missing id returns NotFound(404)") (new TestServer {
    try {
      // Attempt to grab a dataset at zero. This should be
      // converted to an int successfully but cannot be created
      // by the id gen.
      val response = get(s"/$APIVersion/dataset/0")
      assert(response.contentType === Some(JsonHeader))
      assert(response.status === Status.NotFound)
      assert(!response.contentString.isEmpty)
    } finally {
      assertClose()
    }
  })

  test("GET /v1.0/dataset/id non-int returns empty NotFound(404)") (new TestServer {
    try {
      // Attempt to grab a dataset at 'asdf'. This should be
      // converted to an int successfully but cannot be created
      // by the id gen.
      val response = get(s"/$APIVersion/dataset/asdf")
      assert(response.status === Status.NotFound)
      assert(response.contentString.isEmpty)
    } finally {
      assertClose()
    }
  })

  test("POST /v1.0/dataset/id responds Ok(200)") (new TestServer {
    try {
      val TypeMap = """{"w":"x", "y":"z"}"""
      val TestStr = Random.alphanumeric take 10 mkString
      val PauseTime = 2000

      postAndReturn(this, Resource, "{}", "") match {
        case Success(ds) =>
          // wait for the clock to tick
          Thread.sleep(PauseTime)

          // build a request to modify the dataset...
          val request = RequestBuilder()
            .url(fullUrl(s"/$APIVersion/dataset/${ds.id}"))
            .addFormElement("description" -> TestStr)
            .addFormElement("typeMap" -> TypeMap)
            .buildFormPost(multipart = false)

          // send the request and make sure it executes
          val response = Await.result(client(request))
          assert(response.contentType === Some(JsonHeader))
          assert(response.status === Status.Ok)
          assert(!response.contentString.isEmpty)

          // ensure that the data is correct...
          val patchDS = parse(response.contentString).extract[DataSet]
          assert(patchDS.description === TestStr)
          assert(patchDS.dateCreated !== patchDS.dateModified)
          assert(patchDS.typeMap.get("w") === Some("x"))
          assert(patchDS.typeMap.get("y") === Some("z"))

        case Failure(err) =>
          throw new Exception("Failed to create test resource")
      }
    } finally {
      assertClose()
    }
  })

  test("DELETE /v1.0/dataset/id responds Ok(200)") (new TestServer {
    try {
      val TypeMap = """{"w":"x", "y":"z"}"""
      val TestStr = Random.alphanumeric take 10 mkString

      postAndReturn(this, Resource, TypeMap, TestStr) match {
        case Success(ds) =>

          // build a request to modify the dataset...
          val resource = s"/$APIVersion/dataset/${ds.id}"

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
