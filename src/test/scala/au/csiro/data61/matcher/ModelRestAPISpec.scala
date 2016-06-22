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


import java.io.{File, FileInputStream, IOException, ObjectInputStream}
import java.nio.file.Paths

import au.csiro.data61.matcher.types.ModelTypes.{Model, ModelID}
import au.csiro.data61.matcher.types.{FeaturesConfig, MatcherJsonFormats, ModelTypes}
import com.twitter.finagle.http.RequestBuilder
import com.twitter.finagle.http._
import com.twitter.io.Buf
import com.twitter.util.Await
import org.apache.commons.io.FileUtils
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.scalatest.junit.JUnitRunner
import api._
import com.nicta.dataint.matcher.serializable.SerializableMLibClassifier

import language.postfixOps
import scala.util.{Failure, Random, Success, Try}
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
    FileUtils.deleteDirectory(new File(Config.ModelStorageDir)) // this does not update cache

  }

  override def afterEach() {
    FileUtils.deleteDirectory(new File(Config.ModelStorageDir))  // this does not update cache
  }

  def randomString: String = Random.alphanumeric take 10 mkString

  def defaultClasses: List[String] = List.fill(DefaultLabelCount)(randomString)

  def defaultFeatures: JObject =
    ("activeFeatures" -> Seq("num-unique-vals", "prop-unique-vals", "prop-missing-vals" )) ~
    ("activeFeatureGroups" -> Seq("stats-of-text-length", "prop-instances-per-class-in-knearestneighbours")) ~
    ("featureExtractorParams" -> Seq(
      ("name" -> "prop-instances-per-class-in-knearestneighbours") ~
        ("num-neighbours" -> 5)))

  def defaultCostMatrix: JArray =
    JArray(List(JArray(List(1,0,0)), JArray(List(0,1,0)), JArray(List(0,0,1))))

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
    * @param classes The model request object
    * @param description Optional description
    * @return Model that was constructed
    */
  def postAndReturn(classes: List[String],
                    description: Option[String] = None)(implicit s: TestServer): Try[Model] = {

    Try {

      val json =
        ("description" -> description.getOrElse("unknown")) ~
          ("modelType" -> "randomForest") ~
          ("classes" -> classes) ~
          ("features" -> defaultFeatures) ~
          ("costMatrix" -> defaultCostMatrix) ~
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
      val TestClasses = List.fill(LabelLength)(randomString)

      val json =
        ("description" -> TestStr) ~
          ("modelType" -> "randomForest") ~
          ("classes" -> TestClasses) ~
          ("features" -> defaultFeatures) ~
          ("costMatrix" -> defaultCostMatrix) ~
          ("resamplingStrategy" -> "ResampleToMean")

      val request = postRequest(json)

      val response = Await.result(client(request))

      assert(response.contentType === Some(JsonHeader))
      assert(response.status === Status.Ok)
      assert(!response.contentString.isEmpty)
      val model = parse(response.contentString).extract[Model]

      assert(model.description === TestStr)
      assert(model.classes === TestClasses)

    } finally {
      assertClose()
    }
  })

  test("POST /v1.0/model only classes and features required") (new TestServer {
    try {

      val LabelLength = 4
      val TestClasses = List.fill(LabelLength)(randomString)

      val json =
        ("classes" -> TestClasses) ~
          ("features" -> defaultFeatures)

      val request = postRequest(json)

      val response = Await.result(client(request))

      assert(response.contentType === Some(JsonHeader))
      assert(response.status === Status.Ok)
      assert(!response.contentString.isEmpty)
      val model = parse(response.contentString).extract[Model]

      assert(model.classes === TestClasses)
      assert(model.features === FeaturesConfig(
        activeFeatures = Set("num-unique-vals", "prop-unique-vals", "prop-missing-vals"),
        activeGroupFeatures = Set("stats-of-text-length", "prop-instances-per-class-in-knearestneighbours"),
        featureExtractorParams = Map(
          "prop-instances-per-class-in-knearestneighbours" -> Map(
            "name" -> "prop-instances-per-class-in-knearestneighbours",
            "num-neighbours" -> "5")
        )))

    } finally {
      assertClose()
    }
  })

  test("POST /v1.0/model fails if classes not present BadRequest(400)") (new TestServer {
    try {

      // some request without classes...
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

  test("POST /v1.0/model fails if incorrect format BadRequest(400)") (new TestServer {
    try {

      // some request with a bad model type
      val json =
        ("classes" -> defaultClasses) ~
          ("features" -> defaultFeatures) ~
          ("modelType" -> ">>>bad-value<<<")

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
      val TestClasses = List.fill(LabelLength)(randomString)

      postAndReturn(TestClasses) match {
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
      val TestClasses = List.fill(LabelLength)(randomString)

      postAndReturn(TestClasses, Some(TestStr)) match {
        case Success(model) =>

          // build a request to get the model...
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

  test("GET /v1.0/model/1/train returns model not found") (new TestServer {
    try {

      // sending training request
      val trainResp = get(s"/$APIVersion/model/1/train")
      // TODO: check message in response
      assert(trainResp.status === Status.NotFound)
      assert(!trainResp.contentString.isEmpty)

    } finally {
      assertClose()
    }
  })

  test("GET /v1.0/model/some_id/train accepts request") (new TestServer {
    try {
      val response = get(s"/$APIVersion/model") // cache was not updated
      val models : List[ModelID] = Try { parse(response.contentString).extract[List[ModelID]] } match {
          case Success(mods) => mods
          case _   => throw new Exception("No models")
        }

      val someId = models(2) // this model id is in cache, but all directories were deleted
      // sending training request
      val trainResp = get(s"/$APIVersion/model/$someId/train")
      // since model id is still in cache, training request will be accepted
      assert(trainResp.status === Status.Accepted)

      val PauseTime = 1000
      Thread.sleep(PauseTime)

    } finally {
      assertClose()
    }
  })

  test("GET /v1.0/model/some_id/train modifies model train state to error") (new TestServer {
    try {
      val response = get(s"/$APIVersion/model") // cache was not updated
      val models : List[ModelID] = Try { parse(response.contentString).extract[List[ModelID]] } match {
        case Success(mods) => mods
        case _   => throw new Exception("No models")
      }

      val someId = models(1) // this model id is in cache, but all directories were deleted
      // sending training request
      val trainResp = get(s"/$APIVersion/model/$someId/train")
      // since model id is still in cache, training request will be accepted
      assert(trainResp.status === Status.Accepted)
      val PauseTime = 1000
      Thread.sleep(PauseTime)

      // build a request to get the model...
      val modresponse = get(s"/$APIVersion/model/$someId")
      assert(modresponse.contentType === Some(JsonHeader))
      assert(modresponse.status === Status.Ok)
      assert(!modresponse.contentString.isEmpty)
      // since model directories were deleted, training will fail
      val returnedModel = parse(modresponse.contentString).extract[Model]
      assert(returnedModel.state.status === ModelTypes.Status.ERROR)
      assert(!returnedModel.state.message.isEmpty)

    } finally {
      assertClose()
    }
  })

//  //  TODO: train model with no training datasets; I don't get expected results
//  test("GET /v1.0/model/1184298536/train fails with no training datasets") (new TestServer {
//    try {
//      val helperDir = Paths.get("src", "test", "resources").toFile.getAbsolutePath // location for sample files
//      // copy sample model to Config.ModelStorageDir
//      if (!Paths.get(Config.ModelStorageDir).toFile.exists) { // create model storage dir
//        Paths.get(Config.ModelStorageDir).toFile.mkdirs}
//      val mDir = Paths.get(helperDir, "sample.models").toFile // directory to copy from
//      FileUtils.copyDirectory(mDir,                    // copy sample model
//        Paths.get(Config.ModelStorageDir).toFile)
//      if (!Paths.get(Config.DatasetStorageDir).toFile.exists) { // create dataset storage dir
//                Paths.get(Config.DatasetStorageDir).toFile.mkdirs}
//
//      // updating caches explicitly
//      println(get(s"/$APIVersion/model/cache").contentString)
//      get(s"/$APIVersion/model/cache") // update cache for models
//      get(s"/$APIVersion/dataset/cache") // update cache for datasets
//
//      // sending training request
//      val trainResp = get(s"/$APIVersion/model/1184298536/train")
//      println(s"=====Training response: ${trainResp.contentString}")
//      // assert(trainResp.status === Status.Accepted)
//      // TODO: something here wrong; it should be accepted, but it fails
//
//      // wait for the training
//      println("=============sleeping")
//      val PauseTime = 10000
//      Thread.sleep(PauseTime)
//
//      // build a request to get the model...
//      val response = get(s"/$APIVersion/model/1184298536")
//      assert(response.contentType === Some(JsonHeader))
//      assert(response.status === Status.Ok)
//      assert(!response.contentString.isEmpty)
//      // error state
//      val returnedModel = parse(response.contentString).extract[Model]
//      Thread.sleep(1000)
//      println(s"Returned model: ${returnedModel}")
//      println(s"state message = ${returnedModel.state.message}")
//      assert(returnedModel.state.status === ModelTypes.Status.ERROR)
//        // TODO: something wrong; training fails but the status is still UNTRAINED
//
//    } finally {
//      assertClose()
//    }
//  })


  test("GET /v1.0/model/1184298536/train modifies model train state to BUSY") (new TestServer {
    try {
      val helperDir = Paths.get("src", "test", "resources").toFile.getAbsolutePath // location for sample files
      // copy sample model to Config.ModelStorageDir
      if (!Paths.get(Config.ModelStorageDir).toFile.exists) { // create model storage dir
        Paths.get(Config.ModelStorageDir).toFile.mkdirs}
      val mDir = Paths.get(helperDir, "sample.models").toFile // directory to copy from
      FileUtils.copyDirectory(mDir,                    // copy sample model
        Paths.get(Config.ModelStorageDir).toFile)
      // copy sample dataset to Config.DatasetStorageDir
      if (!Paths.get(Config.DatasetStorageDir).toFile.exists) { // create dataset storage dir
        Paths.get(Config.DatasetStorageDir).toFile.mkdirs}
      val dsDir = Paths.get(helperDir, "sample.datasets").toFile // directory to copy from
      FileUtils.copyDirectory(dsDir,                    // copy sample dataset
        Paths.get(Config.DatasetStorageDir).toFile)

      // updating caches explicitly
      get(s"/$APIVersion/model/cache") // update cache for models
      get(s"/$APIVersion/dataset/cache") // update cache for datasets

      // sending training request
      val trainResp = get(s"/$APIVersion/model/1184298536/train")
      assert(trainResp.status === Status.Accepted)

      // build a request to get the model...
      val response = get(s"/$APIVersion/model/1184298536")
      assert(response.contentType === Some(JsonHeader))
      assert(response.status === Status.Ok)
      assert(!response.contentString.isEmpty)
      // train state should be busy
      val returnedModel = parse(response.contentString).extract[Model]
      assert(returnedModel.state.status === ModelTypes.Status.BUSY)

      // wait for the training
      val PauseTime = 10000
      Thread.sleep(PauseTime)

    } finally {
      assertClose()
    }
  })

  test("GET /v1.0/model/1184298536/train returns completed status") (new TestServer {
    try {
      val helperDir = Paths.get("src", "test", "resources").toFile.getAbsolutePath // location for sample files
      // copy sample model to Config.ModelStorageDir
      if (!Paths.get(Config.ModelStorageDir).toFile.exists) { // create model storage dir
        Paths.get(Config.ModelStorageDir).toFile.mkdirs}
      val mDir = Paths.get(helperDir, "sample.models").toFile // directory to copy from
      FileUtils.copyDirectory(mDir,                    // copy sample model
        Paths.get(Config.ModelStorageDir).toFile)
      // copy sample dataset to Config.DatasetStorageDir
      if (!Paths.get(Config.DatasetStorageDir).toFile.exists) { // create dataset storage dir
        Paths.get(Config.DatasetStorageDir).toFile.mkdirs}
      val dsDir = Paths.get(helperDir, "sample.datasets").toFile // directory to copy from
      FileUtils.copyDirectory(dsDir,                    // copy sample dataset
        Paths.get(Config.DatasetStorageDir).toFile)

      // updating caches explicitly
      get(s"/$APIVersion/model/cache") // update cache for models
      get(s"/$APIVersion/dataset/cache") // update cache for datasets

      // sending training request
      val trainResp = get(s"/$APIVersion/model/1184298536/train")
      assert(trainResp.status === Status.Accepted)

      // wait for the training
      val PauseTime = 10000
      Thread.sleep(PauseTime)

      // build a request to get the model...
      val response = get(s"/$APIVersion/model/1184298536")
      assert(response.contentType === Some(JsonHeader))
      assert(response.status === Status.Ok)
      assert(!response.contentString.isEmpty)
      // ensure that the data is correct...
      val returnedModel = parse(response.contentString).extract[Model]
      assert(returnedModel.state.status === ModelTypes.Status.COMPLETE)

    } finally {
      assertClose()
    }
  })

  test("GET /v1.0/model/1184298536/train returns the same model as the data integration project") (new TestServer {
    try {
      val helperDir = Paths.get("src", "test", "resources").toFile.getAbsolutePath // location for sample files
      // copy sample model to Config.ModelStorageDir
      if (!Paths.get(Config.ModelStorageDir).toFile.exists) { // create model storage dir
        Paths.get(Config.ModelStorageDir).toFile.mkdirs}
      val mDir = Paths.get(helperDir, "sample.models").toFile // directory to copy from
      FileUtils.copyDirectory(mDir,                    // copy sample model
        Paths.get(Config.ModelStorageDir).toFile)
      // copy sample dataset to Config.DatasetStorageDir
      if (!Paths.get(Config.DatasetStorageDir).toFile.exists) { // create dataset storage dir
        Paths.get(Config.DatasetStorageDir).toFile.mkdirs}
      val dsDir = Paths.get(helperDir, "sample.datasets").toFile // directory to copy from
      FileUtils.copyDirectory(dsDir,                    // copy sample dataset
        Paths.get(Config.DatasetStorageDir).toFile)

      // updating caches explicitly
      get(s"/$APIVersion/model/cache") // update cache for models
      get(s"/$APIVersion/dataset/cache") // update cache for datasets

      // sending training request
      val trainResp = get(s"/$APIVersion/model/1184298536/train")
      assert(trainResp.status === Status.Accepted)

      // wait for the training
      val PauseTime = 10000
      Thread.sleep(PauseTime)

      // build a request to get the model...
      val response = get(s"/$APIVersion/model/1184298536")
      assert(response.contentType === Some(JsonHeader))
      assert(response.status === Status.Ok)
      assert(!response.contentString.isEmpty)
      // ensure that the data is correct...
      val returnedModel = parse(response.contentString).extract[Model]
      assert(returnedModel.state.status === ModelTypes.Status.COMPLETE)
      // check the content of .rf file
      val learntModelFile = Paths.get(Config.ModelStorageDir, "1184298536", "workspace", "1184298536.rf").toFile
      assert(learntModelFile.exists === true)
      val corFile = Paths.get(helperDir, "1184298536_di.rf").toFile // that's the output from the data integration project
      //      val corFile = Paths.get(helperDir, "1184298536.rf").toFile // that's the output when running API

      // checking that the models are the same; direct comparison of file contents does not yield correct results
      for {
        inLearnt <- Try( new ObjectInputStream(new FileInputStream(learntModelFile)))
          .orElse(Failure( new IOException("Error opening model file.")))
        dataLearnt <- Try(inLearnt.readObject().asInstanceOf[SerializableMLibClassifier])
          .orElse(Failure( new IOException("Error reading model file.")))
        inCor <- Try( new ObjectInputStream(new FileInputStream(corFile)))
          .orElse(Failure( new IOException("Error opening model file.")))
        dataCor <- Try(inCor.readObject().asInstanceOf[SerializableMLibClassifier])
          .orElse(Failure( new IOException("Error reading model file.")))
      } yield{
        assert(dataLearnt.classes === dataCor.classes)
        assert(dataLearnt.model === dataCor.model)
      }

    } finally {
      assertClose()
    }
  })

  test("GET /v1.0/model/some_id/train fails with no data") (new TestServer {
    try {
      val LabelLength = 4
      val TestClasses = List.fill(LabelLength)(randomString)

      postAndReturn(TestClasses) match {
        case Success(model) =>

          // build a request to train the model
          val trainResp = get(s"/$APIVersion/model/${model.id}/train")
          assert(trainResp.status === Status.Accepted)

          // wait for the training
          val PauseTime = 10000
          Thread.sleep(PauseTime)

          // build a request to get the model...
          val response = get(s"/$APIVersion/model/${model.id}")
          assert(response.contentType === Some(JsonHeader))
          assert(response.status === Status.Ok)
          assert(!response.contentString.isEmpty)
          // ensure that the state is error
          val returnedModel = parse(response.contentString).extract[Model]
          println(returnedModel.state.message)
          assert(returnedModel.state.status === ModelTypes.Status.ERROR)

        case Failure(err) =>
          throw new Exception("Failed to create test resource")
      }
    } finally {
      assertClose()
    }
  })


  test("POST /v1.0/model/id responds Ok(200)") (new TestServer {
    try {
      val LabelLength = 4
      val TestStr = randomString
      val TestClasses = List.fill(LabelLength)(randomString)
      val PauseTime = 2000

      val NewDescription = randomString
      val NewClasses = List.fill(LabelLength)(randomString)

      postAndReturn(TestClasses, Some(TestStr)) match {
        case Success(ds) =>
          // wait for the clock to tick for the dateModified
          Thread.sleep(PauseTime)

          val json =
            ("description" -> NewDescription) ~
              ("modelType" -> "randomForest") ~
              ("classes" -> NewClasses) ~
              ("features" -> defaultFeatures ) ~
              ("costMatrix" -> defaultCostMatrix) ~
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
          assert(patchModel.classes === NewClasses)
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
      val NumClasses = 4
      val TestClasses = List.fill(NumClasses)(randomString)

      postAndReturn(TestClasses, Some(TestStr)) match {
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

  test("GET /v1.0/model/1184298536/train does not retrain the model") (new TestServer {
    try {
      val helperDir = Paths.get("src", "test", "resources").toFile.getAbsolutePath // location for sample files
      // copy sample model to Config.ModelStorageDir
      if (!Paths.get(Config.ModelStorageDir).toFile.exists) { // create model storage dir
        Paths.get(Config.ModelStorageDir).toFile.mkdirs}
      val mDir = Paths.get(helperDir, "sample.models").toFile // directory to copy from
      FileUtils.copyDirectory(mDir,                    // copy sample model
        Paths.get(Config.ModelStorageDir).toFile)
      // copy sample dataset to Config.DatasetStorageDir
      if (!Paths.get(Config.DatasetStorageDir).toFile.exists) { // create dataset storage dir
        Paths.get(Config.DatasetStorageDir).toFile.mkdirs}
      val dsDir = Paths.get(helperDir, "sample.datasets").toFile // directory to copy from
      FileUtils.copyDirectory(dsDir,                    // copy sample dataset
        Paths.get(Config.DatasetStorageDir).toFile)

      // updating caches explicitly
      get(s"/$APIVersion/model/cache") // update cache for models
      get(s"/$APIVersion/dataset/cache") // update cache for datasets

      // sending training request
      val trainResp = get(s"/$APIVersion/model/1184298536/train")
      assert(trainResp.status === Status.Accepted)
      // wait for the training
      val PauseTime = 10000
      Thread.sleep(PauseTime)

      // sending training request again
      val trainResp2 = get(s"/$APIVersion/model/1184298536/train")
      assert(trainResp2.status === Status.Accepted)
      // we do not wait for the training, since it should not happen

      // build a request to get the model...
      val response = get(s"/$APIVersion/model/1184298536")
      assert(response.contentType === Some(JsonHeader))
      assert(response.status === Status.Ok)
      assert(!response.contentString.isEmpty)
      // ensure that the data is correct...
      val returnedModel = parse(response.contentString).extract[Model]
      assert(returnedModel.state.status === ModelTypes.Status.COMPLETE)

    } finally {
      assertClose()
    }
  })

  test("GET /v1.0/model/1184298536/train retrains the model after model.json was overwritten") (new TestServer {
    try {
      val helperDir = Paths.get("src", "test", "resources").toFile.getAbsolutePath // location for sample files
      // copy sample model to Config.ModelStorageDir
      if (!Paths.get(Config.ModelStorageDir).toFile.exists) { // create model storage dir
        Paths.get(Config.ModelStorageDir).toFile.mkdirs}
      val mDir = Paths.get(helperDir, "sample.models").toFile // directory to copy from
      FileUtils.copyDirectory(mDir,                    // copy sample model
        Paths.get(Config.ModelStorageDir).toFile)
      // copy sample dataset to Config.DatasetStorageDir
      if (!Paths.get(Config.DatasetStorageDir).toFile.exists) { // create dataset storage dir
        Paths.get(Config.DatasetStorageDir).toFile.mkdirs}
      val dsDir = Paths.get(helperDir, "sample.datasets").toFile // directory to copy from
      FileUtils.copyDirectory(dsDir,                    // copy sample dataset
        Paths.get(Config.DatasetStorageDir).toFile)

      // updating caches explicitly
      get(s"/$APIVersion/model/cache") // update cache for models
      get(s"/$APIVersion/dataset/cache") // update cache for datasets

      // sending training request
      val trainResp = get(s"/$APIVersion/model/1184298536/train")
      assert(trainResp.status === Status.Accepted)
      // wait for the training
      val PauseTime = 10000
      Thread.sleep(PauseTime)

      val json =
        ("description" -> "new change") ~
          ("modelType" -> "randomForest")
      val request = postRequest(json, s"/$APIVersion/model/1184298536")
      Await.result(client(request)) // wait for the files to be overwritten

      // sending training request again
      val trainResp2 = get(s"/$APIVersion/model/1184298536/train")
      assert(trainResp2.status === Status.Accepted)
      // the training should be launched!

      // build a request to get the model and check train status
      val response = get(s"/$APIVersion/model/1184298536")
      assert(response.contentType === Some(JsonHeader))
      assert(response.status === Status.Ok)
      assert(!response.contentString.isEmpty)
      // ensure that the training state is busy
      val returnedModel = parse(response.contentString).extract[Model]
      assert(returnedModel.state.status === ModelTypes.Status.BUSY)
      Thread.sleep(PauseTime) // wait for the training to finish

    } finally {
      assertClose()
    }
  })

//  TODO: create test for bad parameters
//
//  TODO: create model with incorrect featuresconfig feature names?? incorrect feature names are ignored currently both by API and data integration project

}

