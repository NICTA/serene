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


import java.io.{File, FileInputStream, IOException, ObjectInputStream}
import java.nio.file.{Path, Paths}

import au.csiro.data61.core.api.DatasetAPI._
import au.csiro.data61.core.types.ModelTypes.{Model, ModelID}
import au.csiro.data61.core.types._
import au.csiro.data61.core.drivers.ObjectInputStreamWithCustomClassLoader
import com.twitter.finagle.http.RequestBuilder
import com.twitter.finagle.http._
import com.twitter.io.Buf
import com.twitter.util.{Await, Return, Throw}
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FileUtils
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.scalatest.junit.JUnitRunner
import org.scalatest.concurrent._

import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import api._
import au.csiro.data61.core.storage.ModelStorage
import au.csiro.data61.matcher.matcher.serializable.SerializableMLibClassifier
import com.twitter.finagle.http
import org.apache.spark.ml.classification.RandomForestClassificationModel

import language.postfixOps
import scala.annotation.tailrec
import scala.concurrent.Future
import scala.util.{Failure, Random, Success, Try}
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._

/**
 * Tests for the Model REST endpoint API
 */
@RunWith(classOf[JUnitRunner])
class ModelRestAPISpec extends FunSuite with MatcherJsonFormats with BeforeAndAfterEach with Futures with LazyLogging {

  import ModelAPI._

  /**
    * Deletes all the models from the server. Assumes that
    * the IDs are stored as positive integers
    *
    * @param server Reference to the TestServer used in a single test
    */
  def deleteAllModels()(implicit server: TestServer): Unit = {
    val response = server.get(s"/$APIVersion/model")

    if (response.status == Status.Ok) {
      val str = response.contentString
      val regex = "[0-9]+".r
      val models = regex.findAllIn(str).map(_.toInt)
      models.foreach { model =>
        server.delete(s"/$APIVersion/model/$model")
      }
    }
  }

  // we need a dataset server to hold datasets for training...
  val DataSet = new DatasetRestAPISpec
  val TypeMap = """{"a":"b", "c":"d"}"""
  val Resource = DataSet.Resource

  def randomString: String = Random.alphanumeric take 10 mkString

  def defaultFeatures: JObject =
    ("activeFeatures" -> Seq("num-unique-vals", "prop-unique-vals", "prop-missing-vals" )) ~
    ("activeFeatureGroups" -> Seq("stats-of-text-length", "prop-instances-per-class-in-knearestneighbours")) ~
    ("featureExtractorParams" -> Seq(
      ("name" -> "prop-instances-per-class-in-knearestneighbours") ~
        ("num-neighbours" -> 5)))

  def defaultCostMatrix: JArray =
    JArray(List(JArray(List(1,0,0)), JArray(List(0,1,0)), JArray(List(0,0,1))))

  def defaultDataSet: String = getClass.getResource("/homeseekers.csv").getPath

  // default classes for the homeseekers dataset
  def defaultClasses: List[String] = List(
    "unknown",
    "year_built",
    "address",
    "bathrooms",
    "bedrooms",
    "email",
    "fireplace",
    "firm_name",
    "garage",
    "heating",
    "house_description",
    "levels",
    "mls",
    "phone",
    "price",
    "size",
    "type"
  )

  // index labels for the default homeseekers dataset
  def defaultLabels: Map[Int, String] =
    Map(
      4  -> "address",
      5  -> "firm_name",
      7  -> "email",
      9  -> "price",
      10 -> "type",
      11 -> "mls",
      12 -> "levels",
      14 -> "phone",
      18 -> "phone",
      19 -> "year_built",
      21 -> "garage",
      24 -> "fireplace",
      25 -> "bathrooms",
      27 -> "size",
      29 -> "house_description",
      31 -> "phone",
      30 -> "heating",
      32 -> "bedrooms"
    )

//  val helperDir = Paths.get("src", "test", "resources", "helper").toFile.getAbsolutePath // location for sample files
  val helperDir = getClass.getResource("/helper").getPath
//  Paths.get("src", "test", "resources", "helper").toFile.getAbsolutePath // location for sample files

  def copySampleDatasets(): Unit = {
    // copy sample dataset to Config.DatasetStorageDir
    if (!Paths.get(Serene.config.datasetStorageDir).toFile.exists) { // create dataset storage dir
      Paths.get(Serene.config.datasetStorageDir).toFile.mkdirs}
    val dsDir = Paths.get(helperDir, "sample.datasets").toFile // directory to copy from
    FileUtils.copyDirectory(dsDir,                    // copy sample dataset
      Paths.get(Serene.config.datasetStorageDir).toFile)
  }

  def copySampleModels(): Unit = {
    // copy sample model to Config.ModelStorageDir
    if (!Paths.get(Serene.config.modelStorageDir).toFile.exists) { // create model storage dir
      Paths.get(Serene.config.modelStorageDir).toFile.mkdirs}
    val mDir = Paths.get(helperDir, "sample.models").toFile // directory to copy from
    FileUtils.copyDirectory(mDir,                    // copy sample model
      Paths.get(Serene.config.modelStorageDir).toFile)
  }

  def copySampleFiles(): Unit = {
    copySampleDatasets()
    copySampleModels()
  }

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
    * @param labelDataMap Optional map for column labels
    * @param numBags Optional integer numBags
    * @param bagSize OPtional integer bagSize
    * @return Model that was constructed
    */
  def createModel(classes: List[String],
                  description: Option[String] = None,
                  labelDataMap: Option[Map[String, String]] = None,
                  resamplingStrategy: String = "ResampleToMean",
                  numBags: Option[Int] = None,
                  bagSize: Option[Int] = None)(implicit s: TestServer): Try[Model] = {

    Try {

      val json =
        ("description" -> description.getOrElse("unknown")) ~
          ("modelType" -> "randomForest") ~
          ("classes" -> classes) ~
          ("features" -> defaultFeatures) ~
          ("costMatrix" -> defaultCostMatrix) ~
          ("resamplingStrategy" -> resamplingStrategy) ~
          ("numBags" -> numBags) ~
          ("bagSize" -> bagSize)

      // add the labelData if available...
      val labelJson = labelDataMap.map { m =>
        json ~ ("labelData" -> m)
      }.getOrElse(json)

      val req = postRequest(labelJson)

      val response = Await.result(s.client(req))

      parse(response.contentString).extract[Model]
    }
  }

  /**
    * createDataSet creates a single simple dataset from the medium.csv file
    * in the DataSet test spec.
    *
    * @param server The server object
    * @return List of column IDs...
    */
  def createDataSet(server: TestServer): DataSet = {
    // first we add a dataset...
    DataSet.createDataset(server, defaultDataSet, TypeMap, "homeseekers") match {
      case Success(ds) =>
        ds
      case _ =>
        throw new Exception("Failed to create dataset")
    }
  }

  /**
    * createLabelMap creates the default labels from the
    * created dataset...
    *
    * @param ds The dataset that was created
    * @return
    */
  def createLabelMap(ds: DataSet): Map[String, String] = {
    // next grab the columns
    val cols = ds.columns.map(_.id.toString)

    // now we create the colId -> labelMap
    defaultLabels.map { case (i, v) =>
      cols(i) -> v
    }
  }

  /**
    * pollModelState
    *
    * @param model
    * @param pollIterations
    * @param pollTime
    * @param s
    * @return
    */
  def pollModelState(model: Model, pollIterations: Int, pollTime: Int)(implicit s: TestServer): Future[ModelTypes.Status] = {
    Future {

      def state(): ModelTypes.Status = {
        Thread.sleep(pollTime)
        // build a request to get the model...
        val response = s.get(s"/$APIVersion/model/${model.id}")
        if (response.status != Status.Ok) {
          throw new Exception("Failed to retrieve model state")
        }
        // ensure that the data is correct...
        val m = parse(response.contentString).extract[Model]

        m.state.status
      }

      @tailrec
      def rState(loops: Int): ModelTypes.Status = {
        state() match {
          case s@ModelTypes.Status.COMPLETE =>
            s
          case s@ModelTypes.Status.ERROR =>
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
    * This helper function will start the training...
 *
    * @param server
    * @return
    */
  def trainDefault(resamplingStrategy: String = "ResampleToMean",
                   numBags: Option[Int] = None,
                   bagSize: Option[Int] = None)(implicit server: TestServer): (Model, DataSet) = {
    val TestStr = randomString

    // first we add a simple dataset
    val ds = createDataSet(server)
    val labelMap = createLabelMap(ds)

    // next we train the dataset
    createModel(defaultClasses, Some(TestStr), Some(labelMap), resamplingStrategy, numBags, bagSize) match {

      case Success(model) =>

        val request = RequestBuilder()
          .url(server.fullUrl(s"/$APIVersion/model/${model.id}/train"))
          .addHeader("Content-Type", "application/json")
          .buildPost(Buf.Utf8(""))

        // send the request and make sure it executes
        val response = Await.result(server.client(request))

        assert(response.status === Status.Accepted)
        assert(response.contentString.isEmpty)

        (model, ds)
      case Failure(err) =>
        throw new Exception("Failed to create test resource")
    }
  }


  //=========================Tests==============================================

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
      deleteAllModels()
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
        deleteAllModels()
      assertClose()
    }
  })

  test("POST /v1.0/model with incorrect resampling strategy fails") (new TestServer {
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
          ("resamplingStrategy" -> "Resample")

      val request = postRequest(json)

      val response = Await.result(client(request))

      assert(response.contentType === Some(JsonHeader))
      assert(response.status === Status.BadRequest)
      assert(!response.contentString.isEmpty)

    } finally {
      deleteAllModels()
      assertClose()
    }
  })

  test("POST /v1.0/model with bagging resampling strategy") (new TestServer {
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
          ("resamplingStrategy" -> "Bagging") ~
          ("numBags" -> 10) ~
          ("bagSize" -> 1000)

      val request = postRequest(json)

      val response = Await.result(client(request))

      assert(response.contentType === Some(JsonHeader))
      assert(response.status === Status.Ok)
      assert(!response.contentString.isEmpty)

      val model = parse(response.contentString).extract[Model]

      assert(model.description === TestStr)
      assert(model.classes === TestClasses)
      assert(model.resamplingStrategy === SamplingStrategy.BAGGING)
      assert(model.bagSize === Some(1000))
      assert(model.numBags === Some(10))

    } finally {
      deleteAllModels()
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
        deleteAllModels()
      assertClose()
    }
  })

  test("POST /v1.0/model should create correct refDataSets") (new TestServer {
    try {
      val TestStr = randomString
      val PollTime = 1000
      val PollIterations = 10

      // first we add a simple dataset
      val ds = createDataSet(this)

      val labelMap = createLabelMap(ds)

      // next we train the dataset
      createModel(defaultClasses, Some(TestStr), Some(labelMap)) match {

        case Success(model) =>

          val response = get(s"/$APIVersion/model/${model.id}")

          val m = parse(response.contentString).extract[Model]

          assert(m.refDataSets.nonEmpty)
          assert(m.refDataSets.contains(ds.id))

        case Failure(err) =>
          throw new Exception("Failed to create test resource")
      }
    } finally {
      deleteAllModels()
      DataSet.deleteAllDataSets()
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
        deleteAllModels()
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
        deleteAllModels()
      assertClose()
    }
  })

  test("POST /v1.0/model appears in model list") (new TestServer {
    try {
      val LabelLength = 4
      val TestClasses = List.fill(LabelLength)(randomString)

      createModel(TestClasses) match {
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
        deleteAllModels()
      assertClose()
    }
  })

  test("GET /v1.0/model/id responds Ok(200)") (new TestServer {
    try {
      val LabelLength = 4
      val TestStr = randomString
      val TestClasses = List.fill(LabelLength)(randomString)

      createModel(TestClasses, Some(TestStr)) match {
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
        deleteAllModels()
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
        deleteAllModels()
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
        deleteAllModels()
      assertClose()
    }
  })

  test("POST /v1.0/model/id model update responds Ok(200)") (new TestServer {
    try {
      val LabelLength = 4
      val TestStr = randomString
      val TestClasses = List.fill(LabelLength)(randomString)
      val PauseTime = 2000

      val NewDescription = randomString
      val NewClasses = List.fill(LabelLength)(randomString)

      createModel(TestClasses, Some(TestStr)) match {
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
        deleteAllModels()
      assertClose()
    }
  })

  test("DELETE /v1.0/model/id responds Ok(200)") (new TestServer {
    try {
      val TestStr = randomString
      val NumClasses = 4
      val TestClasses = List.fill(NumClasses)(randomString)

      createModel(TestClasses, Some(TestStr)) match {
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
        deleteAllModels()
      assertClose()
    }
  })

  test("DELETE /v1.0/dataset/:id should remove columns and refDataSets from model") (new TestServer {
    try {
      val TestStr = randomString
      val PollTime = 1000
      val PollIterations = 10

      // first we add a simple dataset
      val ds = createDataSet(this)

      val labelMap = createLabelMap(ds)

      // next we train the dataset
      createModel(defaultClasses, Some(TestStr), Some(labelMap)) match {

        case Success(model) =>

          DataSet.deleteAllDataSets()

          val response = get(s"/$APIVersion/model/${model.id}")

          val m = parse(response.contentString).extract[Model]

          assert(m.refDataSets.isEmpty)
          assert(m.labelData.isEmpty)

        case Failure(err) =>
          throw new Exception("Failed to create test resource")
      }
    } finally {
      deleteAllModels()
      DataSet.deleteAllDataSets()
      assertClose()
    }
  })

  //==============================================================================
  // Tests for model training endpoint

  test("POST /v1.0/model/1/train returns model not found") (new TestServer {
    try {

      // sending training request
      val trainResp = RequestBuilder()
        .url(s.fullUrl(s"/$APIVersion/model/1/train"))
        .addHeader("Content-Type", "application/json")
        .buildPost(Buf.Utf8(""))

      val response = Await.result(client(trainResp))

      assert(response.status === Status.NotFound)
      assert(response.contentString.nonEmpty)

    } finally {
      assertClose()
    }
  })

  test("POST /v1.0/model/:id/train accepts request and completes successfully") (new TestServer {
    try {
      val PollTime = 2000
      val PollIterations = 20

      val (model, _) = trainDefault()
      val trained = pollModelState(model, PollIterations, PollTime)

      val state = concurrent.Await.result(trained, 30 seconds)

      assert(state === ModelTypes.Status.COMPLETE)

    } finally {
      deleteAllModels()
      DataSet.deleteAllDataSets()
      assertClose()
    }
  })

  test("POST /v1.0/model/:id/train with bagging accepts request and completes successfully") (new TestServer {
    try {
      val PollTime = 2000
      val PollIterations = 20

      val (model, _) = trainDefault(resamplingStrategy="Bagging", bagSize=Some(100), numBags=Some(10))
      val trained = pollModelState(model, PollIterations, PollTime)

      val state = concurrent.Await.result(trained, 30 seconds)

      assert(state === ModelTypes.Status.COMPLETE)

    } finally {
      deleteAllModels()
      DataSet.deleteAllDataSets()
      assertClose()
    }
  })

  test("POST /v1.0/model/:id/train on a model with no labels should return an error") (new TestServer {
    try {
      val TestStr = randomString
      val PollTime = 1000
      val PollIterations = 10

      // next we train the dataset
      createModel(defaultClasses, Some(TestStr), Some(Map.empty[String, String])) match {

        case Success(model) =>

          val request = RequestBuilder()
            .url(s.fullUrl(s"/$APIVersion/model/${model.id}/train"))
            .addHeader("Content-Type", "application/json")
            .buildPost(Buf.Utf8(""))

          // send the request and make sure it executes
          val response = Await.result(client(request))

          val trained = pollModelState(model, PollIterations, PollTime)

          val state = concurrent.Await.result(trained, 15 seconds)

          assert(response.status === Status.Accepted)
          assert(response.contentString.isEmpty)
          assert(state === ModelTypes.Status.ERROR)

        case Failure(err) =>
          throw new Exception("Failed to create test resource")
      }
    } finally {
      deleteAllModels()
      DataSet.deleteAllDataSets()
      assertClose()
    }
  })

  test("POST /v1.0/model/:id/train should immediately return busy") (new TestServer {
    try {
      val PollTime = 1000
      val PollIterations = 10

      val (model, _) = trainDefault()

      val busyResponse = get(s"/$APIVersion/model/${model.id}")
      val busyModel = parse(busyResponse.contentString).extract[Model]
      assert(busyModel.state.status === ModelTypes.Status.BUSY)

      // now just make sure it completes...
      val trained = pollModelState(model, PollIterations, PollTime)
      val state = concurrent.Await.result(trained, 15 seconds)

      assert(state === ModelTypes.Status.COMPLETE)

    } finally {
      deleteAllModels()
      DataSet.deleteAllDataSets()
      assertClose()
    }
  })

  test("POST /v1.0/model/:id/train once trained should return cached value immediately") (new TestServer {
    try {
      val PollTime = 1000
      val PollIterations = 10

      val (model, _) = trainDefault()

      // now just make sure it completes...
      val trained = pollModelState(model, PollIterations, PollTime)
      val state = concurrent.Await.result(trained, 20 seconds)

      assert(state === ModelTypes.Status.COMPLETE)

      // training complete, now check to see that another train
      // uses cached value
      val secondRequest = RequestBuilder()
        .url(s.fullUrl(s"/$APIVersion/model/${model.id}/train"))
        .addHeader("Content-Type", "application/json")
        .buildPost(Buf.Utf8(""))

      // send the request and make sure it executes
      val secondResponse = Await.result(client(secondRequest))
      assert(secondResponse.status === Status.Accepted)
      assert(secondResponse.contentString.isEmpty)

      // now query the model with no delay and make sure it is complete...
      val completeResponse = get(s"/$APIVersion/model/${model.id}")
      val completeModel = parse(completeResponse.contentString).extract[Model]
      assert(completeModel.state.status === ModelTypes.Status.COMPLETE)

    } finally {
      deleteAllModels()
      DataSet.deleteAllDataSets()
      assertClose()
    }
  })

  test("POST /v1.0/model/:id/train once trained should return busy if model is changed") (new TestServer {
    try {
      val PollTime = 1000
      val PollIterations = 10

      val (model, _) = trainDefault()

      // now just make sure it completes...
      val trained = pollModelState(model, PollIterations, PollTime)
      val state = concurrent.Await.result(trained, 15 seconds)

      assert(state === ModelTypes.Status.COMPLETE)

      // now update the model...
      val json = "description" -> "new change"
      val request = postRequest(json, s"/$APIVersion/model/${model.id}")
      Await.result(client(request)) // wait for the files to be overwritten

      // now launch another training event...
      val secondRequest = RequestBuilder()
        .url(s.fullUrl(s"/$APIVersion/model/${model.id}/train"))
        .addHeader("Content-Type", "application/json")
        .buildPost(Buf.Utf8(""))

      // send the request and make sure it executes
      val secondResponse = Await.result(client(secondRequest))
      assert(secondResponse.status === Status.Accepted)
      assert(secondResponse.contentString.isEmpty)

      // now query the model with no delay and make sure it is complete...
      val completeResponse = get(s"/$APIVersion/model/${model.id}")
      val completeModel = parse(completeResponse.contentString).extract[Model]
      assert(completeModel.state.status === ModelTypes.Status.BUSY)

      // now just make sure it completes...
      val finalTrained = pollModelState(model, PollIterations, PollTime)
      val finalState = concurrent.Await.result(finalTrained, 15 seconds)
      assert(finalState === ModelTypes.Status.COMPLETE)

    } finally {
      deleteAllModels()
      DataSet.deleteAllDataSets()
      assertClose()
    }
  })

  test("POST /v1.0/model/:id/train creates default Model file") (new TestServer {
    try {
      val PollTime = 1000
      val PollIterations = 20

      val (model, ds) = trainDefault()

      // now just make sure it completes...
      val trained = pollModelState(model, PollIterations, PollTime)
      val state = concurrent.Await.result(trained, 15 seconds)

      assert(state === ModelTypes.Status.COMPLETE)

      // check the content of .rf file
      val learntModelFile = Paths.get(Serene.config.modelStorageDir, s"${model.id}", "workspace", s"${model.id}.rf").toFile
      assert(learntModelFile.exists === true)

      // pre-computed model from raw data-integration project...
      val corFile = Paths.get(helperDir, "one_core_seed5043.rf").toFile

      // checking that the models are the same; direct comparison of file contents does not yield correct results
      (for {
        inLearnt <- Try( new ObjectInputStreamWithCustomClassLoader(new FileInputStream(learntModelFile)))
        dataLearnt <- Try(inLearnt.readObject().asInstanceOf[SerializableMLibClassifier])
        inCor <- Try( new ObjectInputStreamWithCustomClassLoader(new FileInputStream(corFile)))
        dataCor <- Try(inCor.readObject().asInstanceOf[SerializableMLibClassifier])
      } yield (dataLearnt, dataCor) ) match {
        case Success((data, cor)) =>
          assert(data.classes === cor.classes)
          assert(data.featureExtractors === cor.featureExtractors)
          val rfModel_new = data.model.stages(2).asInstanceOf[RandomForestClassificationModel]
          val rfModel_one = cor.model.stages(2).asInstanceOf[RandomForestClassificationModel]
          assert(rfModel_new.numClasses === rfModel_one.numClasses)
          assert(rfModel_new.numFeatures === rfModel_one.numFeatures)
          assert(rfModel_new.treeWeights === rfModel_one.treeWeights)
          assert(rfModel_new.numTrees === rfModel_one.numTrees)

          assert(rfModel_new.totalNumNodes === rfModel_one.totalNumNodes)
          assert(rfModel_new.featureImportances === rfModel_one.featureImportances)

        case Failure(err) =>
          throw new Exception(err.getMessage)
      }

    } finally {
      deleteAllModels()
      DataSet.deleteAllDataSets()
      assertClose()
    }
  })

  test("POST /v1.0/model/:id/predict/:id returns successfully") (new TestServer {
    try {
      val PollTime = 1000
      val PollIterations = 10

      val (model, ds) = trainDefault()

      // now just make sure it completes...
      val trained = pollModelState(model, PollIterations, PollTime)
      val state = concurrent.Await.result(trained, 15 seconds)

      assert(state === ModelTypes.Status.COMPLETE)

      // now make a prediction
      val request = RequestBuilder()
        .url(s.fullUrl(s"/$APIVersion/model/${model.id}/predict/${ds.id}"))
        .addHeader("Content-Type", "application/json")
        .buildPost(Buf.Utf8(""))

      val response = Await.result(client(request))

      assert(response.status === Status.Ok)

    } finally {
      deleteAllModels()
      DataSet.deleteAllDataSets()
      assertClose()
    }
  })

  test("POST /v1.0/model/:id/predict/:id prediction fails if model is not trained") (new TestServer {
    try {
      val TestStr = randomString

      // first we add a simple dataset
      val ds: DataSet = createDataSet(this)
      val labelMap = createLabelMap(ds)

      // next we create a model...
      createModel(defaultClasses, Some(TestStr), Some(labelMap)) match {

        case Success(model) =>

          val request = RequestBuilder()
            .url(fullUrl(s"/$APIVersion/model/${model.id}/predict/${ds.id}"))
            .addHeader("Content-Type", "application/json")
            .buildPost(Buf.Utf8(""))

          // send the request and make sure it executes
          val response = Await.result(client(request))

          assert(response.status === Status.BadRequest)
          assert(response.contentString.nonEmpty)

        case Failure(err) =>
          throw new Exception("Failed to create test resource")
      }
    } finally {
      deleteAllModels()
      DataSet.deleteAllDataSets()
      assertClose()
    }
  })

  test("POST /v1.0/model/:id/predict/:id returns predictions with validation > 0.9") (new TestServer {
    try {
      val PollTime = 1000
      val PollIterations = 10

      val (model, ds) = trainDefault()

      // now just make sure it completes...
      val trained = pollModelState(model, PollIterations, PollTime)
      val state = concurrent.Await.result(trained, 15 seconds)

      assert(state === ModelTypes.Status.COMPLETE)

      // now make a prediction
      val request = RequestBuilder()
        .url(s.fullUrl(s"/$APIVersion/model/${model.id}/predict/${ds.id}"))
        .addHeader("Content-Type", "application/json")
        .buildPost(Buf.Utf8(""))

      val response = Await.result(client(request))

      assert(response.status === Status.Ok)

      val prediction = parse(response.contentString).extract[DataSetPrediction]

      // these are the training labels...
      val trueLabels = createLabelMap(ds)
        .toList
        .sortBy(_._1)

      // these are the labels that were predicted
      val testLabels = prediction
          .predictions
          .mapValues(_.label)
          .filterKeys(trueLabels.map(_._1).contains)
          .toList
          .sortBy(_._1)

      // check if they are equal. Here there is a
      // office@house_listing column that is misclassified
      // as a business name...
      val score = testLabels
        .zip(trueLabels)
        .map { case (x, y) =>
          if (x == y) 1.0 else 0.0
        }

      val total = score.sum / testLabels.size

      assert(total > 0.9)

    } finally {
      deleteAllModels()
      DataSet.deleteAllDataSets()
      assertClose()
    }
  })

///////////////////////////////////////////////////////////Old

//
//  test("GET /v1.0/model/1184298536/train returns the same model as the data integration project") (new TestServer {
//    try {
//      copySampleFiles
//
//      // updating caches explicitly
//      get(s"/$APIVersion/model/cache") // update cache for models
//      get(s"/$APIVersion/dataset/cache") // update cache for datasets
//
//      // sending training request
//      val trainResp = get(s"/$APIVersion/model/1184298536/train")
//      assert(trainResp.status === Status.Accepted)
//
//      // wait for the training
//      val PauseTime = 10000
//      Thread.sleep(PauseTime)
//
//      // build a request to get the model...
//      val response = get(s"/$APIVersion/model/1184298536")
//      assert(response.contentType === Some(JsonHeader))
//      assert(response.status === Status.Ok)
//      assert(!response.contentString.isEmpty)
//      // ensure that the data is correct...
//      val returnedModel = parse(response.contentString).extract[Model]
//      assert(returnedModel.state.status === ModelTypes.Status.COMPLETE)
//      // check the content of .rf file
//      val learntModelFile = Paths.get(Config.ModelStorageDir, "1184298536", "workspace", "1184298536.rf").toFile
//      assert(learntModelFile.exists === true)
//      val corFile = Paths.get(helperDir, "1184298536_di.rf").toFile // that's the output from the data integration project
//      //      val corFile = Paths.get(helperDir, "1184298536.rf").toFile // that's the output when running API
//
//      // checking that the models are the same; direct comparison of file contents does not yield correct results
//      for {
//        inLearnt <- Try( new ObjectInputStream(new FileInputStream(learntModelFile)))
//          .orElse(Failure( new IOException("Error opening model file.")))
//        dataLearnt <- Try(inLearnt.readObject().asInstanceOf[SerializableMLibClassifier])
//          .orElse(Failure( new IOException("Error reading model file.")))
//        inCor <- Try( new ObjectInputStream(new FileInputStream(corFile)))
//          .orElse(Failure( new IOException("Error opening model file.")))
//        dataCor <- Try(inCor.readObject().asInstanceOf[SerializableMLibClassifier])
//          .orElse(Failure( new IOException("Error reading model file.")))
//      } yield{
//        assert(dataLearnt.classes === dataCor.classes)
//        assert(dataLearnt.model === dataCor.model)
//      }
//
//    } finally {
//      assertClose()
//    }
//  })
//
//  test("GET /v1.0/model/some_id/train fails with no data") (new TestServer {
//    try {
//      val LabelLength = 4
//      val TestClasses = List.fill(LabelLength)(randomString)
//
//      postAndReturn(TestClasses) match {
//        case Success(model) =>
//
//          // build a request to train the model
//          val trainResp = get(s"/$APIVersion/model/${model.id}/train")
//          assert(trainResp.status === Status.Accepted)
//
//          // wait for the training
//          val PauseTime = 10000
//          Thread.sleep(PauseTime)
//
//          // build a request to get the model...
//          val response = get(s"/$APIVersion/model/${model.id}")
//          assert(response.contentType === Some(JsonHeader))
//          assert(response.status === Status.Ok)
//          assert(!response.contentString.isEmpty)
//          // ensure that the state is error
//          val returnedModel = parse(response.contentString).extract[Model]
//          println(returnedModel.state.message)
//          assert(returnedModel.state.status === ModelTypes.Status.ERROR)
//
//        case Failure(err) =>
//          throw new Exception("Failed to create test resource")
//      }
//    } finally {
//      assertClose()
//    }
//  })
//
//  test("GET /v1.0/model/1184298536/train does not retrain the model") (new TestServer {
//    try {
//      copySampleFiles
//
//      // updating caches explicitly
//      get(s"/$APIVersion/model/cache") // update cache for models
//      get(s"/$APIVersion/dataset/cache") // update cache for datasets
//
//      // sending training request
//      val trainResp = get(s"/$APIVersion/model/1184298536/train")
//      assert(trainResp.status === Status.Accepted)
//      // wait for the training
//      val PauseTime = 10000
//      Thread.sleep(PauseTime)
//      val response1 = get(s"/$APIVersion/model/1184298536")
//      // remember the dateModified
//      val dateModified1 = parse(response1.contentString).extract[Model].state.dateModified
//
//      // sending training request again
//      val trainResp2 = get(s"/$APIVersion/model/1184298536/train")
//      assert(trainResp2.status === Status.Accepted)
//      // we do not wait for the training, since it should not happen
//
//      // build a request to get the model...
//      val response = get(s"/$APIVersion/model/1184298536")
//      assert(response.contentType === Some(JsonHeader))
//      assert(response.status === Status.Ok)
//      assert(!response.contentString.isEmpty)
//      // ensure that the data is correct...
//      val returnedModel = parse(response.contentString).extract[Model]
//      assert(returnedModel.state.status === ModelTypes.Status.COMPLETE)
//      // check that dateModified did not change...
//      assert(returnedModel.state.dateModified === dateModified1)
//
//    } finally {
//      assertClose()
//    }
//  })
//
//  test("GET /v1.0/model/1184298536/train retrains the model after model.json was overwritten") (new TestServer {
//    try {
//      copySampleFiles
//
//      // updating caches explicitly
//      get(s"/$APIVersion/model/cache") // update cache for models
//      get(s"/$APIVersion/dataset/cache") // update cache for datasets
//
//      // sending training request
//      val trainResp = get(s"/$APIVersion/model/1184298536/train")
//      assert(trainResp.status === Status.Accepted)
//      // wait for the training
//      val PauseTime = 10000
//      Thread.sleep(PauseTime)
//
//      val json =
//        ("description" -> "new change") ~
//          ("modelType" -> "randomForest")
//      val request = postRequest(json, s"/$APIVersion/model/1184298536")
//      Await.result(client(request)) // wait for the files to be overwritten
//
//      // sending training request again
//      val trainResp2 = get(s"/$APIVersion/model/1184298536/train")
//      assert(trainResp2.status === Status.Accepted)
//      // the training should be launched!
//
//      // build a request to get the model and check train status
//      val response = get(s"/$APIVersion/model/1184298536")
//      assert(response.contentType === Some(JsonHeader))
//      assert(response.status === Status.Ok)
//      assert(!response.contentString.isEmpty)
//      // ensure that the training state is busy
//      val returnedModel = parse(response.contentString).extract[Model]
//      assert(returnedModel.state.status === ModelTypes.Status.BUSY)
//      Thread.sleep(PauseTime) // wait for the training to finish
//
//    } finally {
//      assertClose()
//    }
//  })
//
//  //==============================================================================
//  // Tests for model prediction endpoint
//  test("POST /v1.0/model/1184298536/predict changes model state to BUSY") (new TestServer {
//    try {
//      copySampleFiles
//
//      // updating caches explicitly
//      get(s"/$APIVersion/model/cache") // update cache for models
//      get(s"/$APIVersion/dataset/cache") // update cache for datasets
//
//      // sending training request
//      val trainResp = get(s"/$APIVersion/model/1184298536/train")
//      assert(trainResp.status === Status.Accepted)
//      // wait for the training
//      val PauseTime = 10000
//      Thread.sleep(PauseTime)
//
//      // sending prediction request
//      val predReq = http.Request(http.Method.Post, s"/$APIVersion/model/1184298536/predict")
//      val predResp = Await.result(client(predReq))
//      assert(predResp.status === Status.Accepted)
//      // the prediction should be launched!
//
//      // build a request to get the model and check status
//      val response = get(s"/$APIVersion/model/1184298536")
//      assert(response.contentType === Some(JsonHeader))
//      assert(response.status === Status.Ok)
//      assert(!response.contentString.isEmpty)
//      // ensure that the training state is busy
//      val returnedModel = parse(response.contentString).extract[Model]
//      assert(returnedModel.state.status === ModelTypes.Status.BUSY)
//      Thread.sleep(PauseTime) // wait for the prediction to finish
//
//    } finally {
//      assertClose()
//    }
//  })
//
//  test("POST /v1.0/model/1184298536/predict changes model state to COMPLETE " +
//    "after prediction is over but dateModified is not changed") (new TestServer {
//    try {
//      copySampleFiles
//
//      // updating caches explicitly
//      get(s"/$APIVersion/model/cache") // update cache for models
//      get(s"/$APIVersion/dataset/cache") // update cache for datasets
//
//      // sending training request
//      val trainResp = get(s"/$APIVersion/model/1184298536/train")
//      assert(trainResp.status === Status.Accepted)
//      // wait for the training
//      val PauseTime = 10000
//      Thread.sleep(PauseTime)
//      val response1 = get(s"/$APIVersion/model/1184298536")
//      // remember the dateModified
//      val dateModified1 = parse(response1.contentString).extract[Model].state.dateModified
//
//      // sending prediction request
//      val predReq = http.Request(http.Method.Post, s"/$APIVersion/model/1184298536/predict")
//      val predResp = Await.result(client(predReq))
//      assert(predResp.status === Status.Accepted)
//      // the prediction should be launched!
//      Thread.sleep(PauseTime) // wait for the prediction to finish
//
//      // build a request to get the model and check status
//      val response = get(s"/$APIVersion/model/1184298536")
//      assert(response.contentType === Some(JsonHeader))
//      assert(response.status === Status.Ok)
//      assert(!response.contentString.isEmpty)
//      // ensure that the training state is complete
//      val returnedModel = parse(response.contentString).extract[Model]
//      assert(returnedModel.state.status === ModelTypes.Status.COMPLETE) // status is complete
//      assert(returnedModel.state.dateModified === dateModified1) // dateModified is unchanged
//    } finally {
//      assertClose()
//    }
//  })
//
//  test("POST /v1.0/model/1184298536/predict fails since model is not trained") (new TestServer {
//    try {
//      copySampleFiles
//
//      // updating caches explicitly
//      get(s"/$APIVersion/model/cache") // update cache for models
//      get(s"/$APIVersion/dataset/cache") // update cache for datasets
//
//      val PauseTime = 10000
//      // sending prediction request
//      val predReq = http.Request(http.Method.Post, s"/$APIVersion/model/1184298536/predict")
//      val predResp = Await.result(client(predReq))
//      assert(predResp.status === Status.NotFound)
//      // the prediction should not be launched!
//      Thread.sleep(10) // wait for the prediction to finish
//
//    } finally {
//      assertClose()
//    }
//  })
//
//  test("POST /v1.0/model/1184298536/predict creates exactly one prediction file") (new TestServer {
//    try {
//      copySampleFiles
//      val modelID = 1184298536
//      val datasetID = 59722533
//
//      // updating caches explicitly
//      get(s"/$APIVersion/model/cache") // update cache for models
//      get(s"/$APIVersion/dataset/cache") // update cache for datasets
//
//      // sending training request
//      val trainResp = get(s"/$APIVersion/model/$modelID/train")
//      assert(trainResp.status === Status.Accepted)
//      // wait for the training
//      val PauseTime = 10000
//      Thread.sleep(PauseTime)
//
//      // sending prediction request
//      val predReq = http.Request(http.Method.Post, s"/$APIVersion/model/$modelID/predict")
//      val predResp = Await.result(client(predReq))
//      assert(predResp.status === Status.Accepted)
//      // the prediction should be launched!
//      Thread.sleep(PauseTime) // wait for the prediction to finish
//
//      // only one prediction file should be created
//      val availPreds = ModelStorage.availablePredictions(modelID)
//      assert(availPreds.size === 1)
//      assert(availPreds === List(datasetID))
//    } finally {
//      assertClose()
//    }
//  })
//
//  test("availablePredictions for 1184298536 should be empty") (new TestServer {
//    try {
//      copySampleFiles
//
//      // updating caches explicitly
//      get(s"/$APIVersion/model/cache") // update cache for models
//      get(s"/$APIVersion/dataset/cache") // update cache for datasets
//
//      // only one prediction file should be created
//      val availPreds = ModelStorage.availablePredictions(1184298536)
//      assert(availPreds.size === 0)
//    } finally {
//      assertClose()
//    }
//  })
//
//  test("GET /v1.0/model/1184298536/predict returns successfully predictions") (new TestServer {
//    try {
//      copySampleFiles
//      val modelID = 1184298536
//      val datasetID = 59722533
//
//      // updating caches explicitly
//      get(s"/$APIVersion/model/cache") // update cache for models
//      get(s"/$APIVersion/dataset/cache") // update cache for datasets
//
//      // sending training request
//      val trainResp = get(s"/$APIVersion/model/$modelID/train")
//      assert(trainResp.status === Status.Accepted)
//      // wait for the training
//      val PauseTime = 10000
//      Thread.sleep(PauseTime)
//
//      // sending prediction request
//      val predReq = http.Request(http.Method.Post, s"/$APIVersion/model/$modelID/predict")
//      val predResp = Await.result(client(predReq))
//      assert(predResp.status === Status.Accepted)
//      // the prediction should be launched!
//      Thread.sleep(PauseTime) // wait for the prediction to finish
//
//      // getting predictions
//      val predictionsReq = get(s"/$APIVersion/model/$modelID/predict")
//      assert(predictionsReq.status === Status.Ok)
//      val predictions = parse(predictionsReq.contentString).extract[List[ColumnPrediction]]
//
//
//      val predPath = ModelStorage.getPredictionsPath(modelID) // directory where prediction file should be stored for this model
//      // get number of classes for this model
//      val numClasses = ModelStorage.get(modelID).map(_.classes.size).getOrElse(throw NotFoundException(s"Model not found."))
//      val filePath = Paths.get(predPath.toString, s"$datasetID.csv").toString
//      val readPredictions = ModelPredictor.readPredictions(filePath, numClasses, modelID)
//
//      assert(predictions === readPredictions)
//    } finally {
//      assertClose()
//    }
//  })
//
//  test("GET /v1.0/model/1184298536/train does not launch training:" +
//    " training is not needed after prediction") (new TestServer {
//    try {
//      copySampleFiles
//      val modelID = 1184298536
//      val datasetID = 59722533
//
//      // updating caches explicitly
//      get(s"/$APIVersion/model/cache") // update cache for models
//      get(s"/$APIVersion/dataset/cache") // update cache for datasets
//
//      // sending training request
//      val trainResp = get(s"/$APIVersion/model/$modelID/train")
//      assert(trainResp.status === Status.Accepted)
//      // wait for the training
//      val PauseTime = 10000
//      Thread.sleep(PauseTime)
//      val response1 = get(s"/$APIVersion/model/$modelID") // get model info
//      // remember the dateModified
//      val dateModified1 = parse(response1.contentString).extract[Model].state.dateModified
//
//      // sending prediction request
//      val predReq = http.Request(http.Method.Post, s"/$APIVersion/model/$modelID/predict")
//      val predResp = Await.result(client(predReq))
//      assert(predResp.status === Status.Accepted)
//      // the prediction should be launched!
//      Thread.sleep(PauseTime) // wait for the prediction to finish
//
//      // sending training request again
//      val trainResp2 = get(s"/$APIVersion/model/$modelID/train")
//      assert(trainResp2.status === Status.Accepted)
//      // we do not wait for the training, since it should not happen
//
//      // build a request to get the model...
//      val response = get(s"/$APIVersion/model/$modelID")
//      assert(response.contentType === Some(JsonHeader))
//      assert(response.status === Status.Ok)
//      assert(!response.contentString.isEmpty)
//      // ensure that the data is correct...
//      val returnedModel = parse(response.contentString).extract[Model]
//      assert(returnedModel.state.status === ModelTypes.Status.COMPLETE)
//      // check that dateModified did not change...
//      assert(returnedModel.state.dateModified === dateModified1)
//
//    } finally {
//      assertClose()
//    }
//  })
//
//  test("POST /v1.0/model/1184298536/predict does not launch prediction if prediction is available") (new TestServer {
//    try {
//      val modelID = 1184298536
//      val datasetID = 59722533
//      copySampleFiles
//
//      // updating caches explicitly
//      get(s"/$APIVersion/model/cache") // update cache for models
//      get(s"/$APIVersion/dataset/cache") // update cache for datasets
//
//      // sending training request
//      val trainResp = get(s"/$APIVersion/model/$modelID/train")
//      assert(trainResp.status === Status.Accepted)
//      // wait for the training
//      val PauseTime = 10000
//      Thread.sleep(PauseTime)
//
//      // sending prediction request
//      val predReq = http.Request(http.Method.Post, s"/$APIVersion/model/$modelID/predict")
//      val predResp = Await.result(client(predReq))
//      assert(predResp.status === Status.Accepted)
//      // the prediction should be launched!
//      Thread.sleep(PauseTime) // wait for the prediction to finish
//
//      // getting predictions
//      val predictionsReq = get(s"/$APIVersion/model/$modelID/predict")
//      assert(predictionsReq.status === Status.Ok)
//      val predPath = ModelStorage.getPredictionsPath(modelID) // directory where prediction file should be stored for this model
//      // remember the date when the file with derived features got created
//      val date1 = Paths.get(predPath.toString, s"$datasetID.csv").toFile.lastModified
//
//      // sending prediction request again
//      val predReq2 = http.Request(http.Method.Post, s"/$APIVersion/model/$modelID/predict")
//      val predResp2 = Await.result(client(predReq))
//      assert(predResp2.status === Status.Accepted)
//      // the prediction should not be launched!
//      Thread.sleep(1000) // wait till prediction repo is checked
//
//      // build a request to get the model...
//      val response = get(s"/$APIVersion/model/$modelID")
//      assert(response.contentType === Some(JsonHeader))
//      assert(response.status === Status.Ok)
//      assert(!response.contentString.isEmpty)
//      // ensure that the status is set to complete!
//      val returnedModel = parse(response.contentString).extract[Model]
//      assert(returnedModel.state.status === ModelTypes.Status.COMPLETE)
//
//      // getting predictions
//      val predictionsReq2 = get(s"/$APIVersion/model/$modelID/predict")
//      assert(predictionsReq2.status === Status.Ok)
//
//      // the file with derived features should not be modified!
//      val date2 = Paths.get(predPath.toString, s"$datasetID.csv").toFile.lastModified
//      assert(date1 === date2)
//    } finally {
//      assertClose()
//    }
//  })



  // TODO: prediction is performed again if there are changes to the model
  // TODO: prediction is performed again if there are changes to the dataset

//  TODO: create test for bad parameters
//
//  TODO: create model with incorrect featuresconfig feature names?? incorrect feature names are ignored currently both by API and data integration project

}

