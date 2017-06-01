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
import au.csiro.data61.types.ModelTypes.{Model, ModelID}
import au.csiro.data61.types._
import au.csiro.data61.core.drivers.ObjectInputStreamWithCustomClassLoader
import com.twitter.finagle.http.RequestBuilder
import com.twitter.finagle.http._
import com.twitter.io.Buf
import com.twitter.util.{Await, Return, Throw}
import com.typesafe.scalalogging.LazyLogging
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.scalatest.junit.JUnitRunner
import org.scalatest.concurrent._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import api._
import au.csiro.data61.core.storage.{JsonFormats, ModelStorage}
import au.csiro.data61.matcher.matcher.serializable.SerializableMLibClassifier
import au.csiro.data61.types.DataSetTypes.DataSetID
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
class ModelRestAPISpec extends FunSuite with JsonFormats with BeforeAndAfterEach with Futures with LazyLogging {

  import ModelAPI._

  implicit val version = APIVersion

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

  def fullFeatures: JObject =
    ("activeFeatures" -> Seq("num-unique-vals", "prop-unique-vals", "prop-missing-vals",
      "ratio-alpha-chars", "prop-numerical-chars",
      "prop-whitespace-chars", "prop-entries-with-at-sign",
      "prop-entries-with-hyphen", "prop-entries-with-paren",
      "prop-entries-with-currency-symbol", "mean-commas-per-entry",
      "mean-forward-slashes-per-entry",
      "prop-range-format", "is-discrete", "entropy-for-discrete-values")) ~
      ("activeFeatureGroups" -> Seq("stats-of-text-length", "prop-instances-per-class-in-knearestneighbours",
        "mean-character-cosine-similarity-from-class-examples",
        "min-editdistance-from-class-examples",
        "min-wordnet-jcn-distance-from-class-examples",
        "min-wordnet-lin-distance-from-class-examples")) ~
      ("featureExtractorParams" -> Seq(
        ("name" -> "prop-instances-per-class-in-knearestneighbours") ~
          ("num-neighbours" -> 5),
        ("name" -> "min-wordnet-jcn-distance-from-class-examples") ~
          ("max-comparisons-per-class" -> 5),
        ("name" -> "min-wordnet-lin-distance-from-class-examples") ~
          ("max-comparisons-per-class" -> 5)
      ))

  def charDistFeatures: JObject =
    ("activeFeatures" -> Seq("shannon-entropy" )) ~
      ("activeFeatureGroups" -> Seq("char-dist-features"))

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

  val helperDir = getClass.getResource("/helper").getPath

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
    * @param resamplingStrategy String for the resampling strategy, default="ResampleToMean"
    * @param numBags Optional integer numBags
    * @param bagSize OPtional integer bagSize
    * @param features Json object for feature configuration, by default it takes defaultFeatures
    * @return Model that was constructed
    */
  def createModel(classes: List[String],
                  description: Option[String] = None,
                  labelDataMap: Option[Map[String, String]] = None,
                  resamplingStrategy: String = "ResampleToMean",
                  numBags: Option[Int] = None,
                  bagSize: Option[Int] = None,
                  features: JObject = defaultFeatures)(implicit s: TestServer): Try[Model] = {

    Try {

      val json =
        ("description" -> description.getOrElse("unknown")) ~
          ("modelType" -> "randomForest") ~
          ("classes" -> classes) ~
          ("features" -> features) ~
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
    server.createDataset(Paths.get(defaultDataSet).toFile, "homeseekers", TypeMap) match {
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
  def pollModelState(model: Model, pollIterations: Int, pollTime: Int)(implicit s: TestServer)
  : Future[Training.Status] = {
    Future {

      def state(): Training.Status = {
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
      def rState(loops: Int): Training.Status = {
        state() match {
          case s@Training.Status.COMPLETE =>
            s
          case s@Training.Status.ERROR =>
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
    * @param server Implicit test server instance
    * @param resamplingStrategy String for the resampling strategy, default="ResampleToMean"
    * @param numBags Optional integer numBags
    * @param bagSize OPtional integer bagSize
    * @param features Json object for feature configuration, by default it takes defaultFeatures
    * @return
    */
  def trainDefault(resamplingStrategy: String = "ResampleToMean",
                   numBags: Option[Int] = None,
                   bagSize: Option[Int] = None,
                   features: JObject = defaultFeatures)(implicit server: TestServer): (Model, DataSet) = {
    val TestStr = randomString

    // first we add a simple dataset
    val ds = createDataSet(server)
    val labelMap = createLabelMap(ds)

    // next we train the dataset
    createModel(defaultClasses, Some(TestStr), Some(labelMap),
      resamplingStrategy, numBags, bagSize, features) match {

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
      deleteAllModels
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
      deleteAllModels
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
      deleteAllModels
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
      assert(model.bagSize === 1000)
      assert(model.numBags === 10)

    } finally {
      deleteAllModels
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
        deleteAllModels
      assertClose()
    }
  })

  test("POST /v1.0/model should create correct refDataSets") (new TestServer {
    try {
      val TestStr = randomString

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
      deleteAllModels
      deleteAllDatasets
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
        deleteAllModels
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
        deleteAllModels
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
        deleteAllModels
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
        deleteAllModels
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
      deleteAllModels
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
        deleteAllModels
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
        deleteAllModels
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
      deleteAllModels
      assertClose()
    }
  })

  test("DELETE /v1.0/dataset/:id will not delete dataset since model depends on it") (new TestServer {
    try {
      val TestStr = randomString
      // first we add a simple dataset
      val ds = createDataSet(this)

      val labelMap = createLabelMap(ds)

      // next we train the dataset
      createModel(defaultClasses, Some(TestStr), Some(labelMap)) match {

        case Success(model) =>

          deleteAllDatasets // dataset will not be removed

          val datasets: List[DataSetID] = parse(get(s"/$APIVersion/dataset").contentString).extract[List[DataSetID]]
          assert(datasets === List(ds.id))

          val response = get(s"/$APIVersion/model/${model.id}")

          val m = parse(response.contentString).extract[Model]

          assert(m.refDataSets === model.refDataSets)
          assert(m.labelData === model.labelData)

        case Failure(err) =>
          throw new Exception("Failed to create test resource")
      }
    } finally {
      deleteAllModels
      deleteAllDatasets
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
      val PollTime = 1000
      val PollIterations = 60

      val (model, _) = trainDefault()
      val trained = pollModelState(model, PollIterations, PollTime)

      val state = concurrent.Await.result(trained, PollTime * PollIterations * 2 seconds)

      assert(state === Training.Status.COMPLETE)

    } finally {
      deleteAllModels
      deleteAllDatasets
      assertClose()
    }
  })

  test("POST /v1.0/model/:id/train with character distribution vector accepts request and completes successfully") (new TestServer {
    try {
      val PollTime = 1000
      val PollIterations = 60

      val (model, _) = trainDefault(features = charDistFeatures)
      val trained = pollModelState(model, PollIterations, PollTime)

      val state = concurrent.Await.result(trained, PollTime * PollIterations * 2 seconds)

      assert(state === Training.Status.COMPLETE)

    } finally {
      deleteAllModels
      deleteAllDatasets
      assertClose()
    }
  })

  test("POST /v1.0/model/:id/train with bagging accepts request and completes successfully") (new TestServer {
    try {
      val PollTime = 1000
      val PollIterations = 60

      val (model, _) = trainDefault(resamplingStrategy="Bagging", bagSize=Some(100), numBags=Some(10))
      val trained = pollModelState(model, PollIterations, PollTime)

      val state = concurrent.Await.result(trained, PollTime * PollIterations * 2 seconds)

      assert(state === Training.Status.COMPLETE)

    } finally {
      deleteAllModels
      deleteAllDatasets
      assertClose()
    }
  })

  test("POST /v1.0/model/:id/train on a model with no labels should return an error") (new TestServer {
    try {
      val TestStr = randomString
      val PollTime = 1000
      val PollIterations = 20

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

          val state = concurrent.Await.result(trained, PollTime * PollIterations * 2 seconds)

          assert(response.status === Status.Accepted)
          assert(response.contentString.isEmpty)
          assert(state === Training.Status.ERROR)

        case Failure(err) =>
          throw new Exception("Failed to create test resource")
      }
    } finally {
      deleteAllModels
      deleteAllDatasets
      assertClose()
    }
  })

  test("POST /v1.0/model/:id/train should immediately return busy") (new TestServer {
    try {
      val PollTime = 1000
      val PollIterations = 20

      val (model, _) = trainDefault()

      val busyResponse = get(s"/$APIVersion/model/${model.id}")
      val busyModel = parse(busyResponse.contentString).extract[Model]
      assert(busyModel.state.status === Training.Status.BUSY)

      // now just make sure it completes...
      val trained = pollModelState(model, PollIterations, PollTime)
      val state = concurrent.Await.result(trained, PollTime * PollIterations * 2 seconds)

      assert(state === Training.Status.COMPLETE)

    } finally {
      deleteAllModels
      deleteAllDatasets
      assertClose()
    }
  })

  test("POST /v1.0/model/:id/train once trained should return cached value immediately") (new TestServer {
    try {
      val PollTime = 1000
      val PollIterations = 20

      val (model, _) = trainDefault()

      // now just make sure it completes...
      val trained = pollModelState(model, PollIterations, PollTime)
      val state = concurrent.Await.result(trained, PollTime * PollIterations * 2 seconds)

      assert(state === Training.Status.COMPLETE)

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
      assert(completeModel.state.status === Training.Status.COMPLETE)

    } finally {
      deleteAllModels
      deleteAllDatasets
      assertClose()
    }
  })

  test("POST /v1.0/model/:id/train once trained should return busy if model is changed") (new TestServer {
    try {
      val PollTime = 1000
      val PollIterations = 60

      val (model, _) = trainDefault()

      // now just make sure it completes...
      val trained = pollModelState(model, PollIterations, PollTime)
      val state = concurrent.Await.result(trained, PollTime * PollIterations * 2 seconds)

      assert(state === Training.Status.COMPLETE)

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
      assert(completeModel.state.status === Training.Status.BUSY)

      // now just make sure it completes...
      val finalTrained = pollModelState(model, PollIterations, PollTime)
      val finalState = concurrent.Await.result(finalTrained, 15 seconds)
      assert(finalState === Training.Status.COMPLETE)

    } finally {
      deleteAllModels
      deleteAllDatasets
      assertClose()
    }
  })

  test("POST /v1.0/model/:id/train creates default Model file with default features and NoResampling") (new TestServer {
    try {
      val PollTime = 1000
      val PollIterations = 60

      val (model, ds) = trainDefault(resamplingStrategy="NoResampling", features = defaultFeatures)

      // now just make sure it completes...
      val trained = pollModelState(model, PollIterations, PollTime)
      val state = concurrent.Await.result(trained, PollTime * PollIterations * 2 seconds)

      assert(state === Training.Status.COMPLETE)

      // check the content of .rf file
      val learntModelFile = Paths.get(
        Serene.config.storageDirs.model, s"${model.id}", "workspace", s"${model.id}.rf").toFile
      assert(learntModelFile.exists === true)

      // pre-computed model with default spark config
      val corFile = Paths.get(helperDir, "defaultfeatures_noresampling_spark2.rf").toFile

      // checking that the models are the same; direct comparison of file contents does not yield correct results
      (for {
        inLearnt <- Try( new ObjectInputStreamWithCustomClassLoader(new FileInputStream(learntModelFile)))
        dataLearnt <- Try(inLearnt.readObject().asInstanceOf[SerializableMLibClassifier])
        inCor <- Try( new ObjectInputStreamWithCustomClassLoader(new FileInputStream(corFile)))
        dataCor <- Try(inCor.readObject().asInstanceOf[SerializableMLibClassifier])
      } yield (dataLearnt, dataCor) ) match {
        case Success((data, cor)) =>
          assert(data.classes === cor.classes)
          val rfModel_new = data.model.stages(2).asInstanceOf[RandomForestClassificationModel]
          val rfModel_one = cor.model.stages(2).asInstanceOf[RandomForestClassificationModel]
          assert(rfModel_new.numClasses === rfModel_one.numClasses)
          assert(rfModel_new.numFeatures === rfModel_one.numFeatures)
          assert(rfModel_new.treeWeights === rfModel_one.treeWeights)
//          assert(rfModel_new.numTrees === rfModel_one.numTrees)

          assert(rfModel_new.totalNumNodes === rfModel_one.totalNumNodes)
          assert(rfModel_new.featureImportances === rfModel_one.featureImportances)
          assert(data.featureExtractors === cor.featureExtractors)

        case Failure(err) =>
          throw new Exception(err.getMessage)
      }

    } finally {
      deleteAllModels
      deleteAllDatasets
      assertClose()
    }
  })

  test("POST /v1.0/model/:id/train creates default Model file with full features and NoResampling") (new TestServer {
    try {
      val PollTime = 1000
      val PollIterations = 60

      val (model, ds) = trainDefault(resamplingStrategy="NoResampling", features = fullFeatures)

      // now just make sure it completes...
      val trained = pollModelState(model, PollIterations, PollTime)
      val state = concurrent.Await.result(trained, PollTime * PollIterations * 2 seconds)

      assert(state === Training.Status.COMPLETE)

      // check the content of .rf file
      val learntModelFile = Paths.get(
        Serene.config.storageDirs.model, s"${model.id}", "workspace", s"${model.id}.rf").toFile
      assert(learntModelFile.exists === true)

      // pre-computed model with default spark config
      val corFile = Paths.get(helperDir, "fullfeatures_noresampling_spark2.rf").toFile

      // checking that the models are the same; direct comparison of file contents does not yield correct results
      (for {
        inLearnt <- Try( new ObjectInputStreamWithCustomClassLoader(new FileInputStream(learntModelFile)))
        dataLearnt <- Try(inLearnt.readObject().asInstanceOf[SerializableMLibClassifier])
        inCor <- Try( new ObjectInputStreamWithCustomClassLoader(new FileInputStream(corFile)))
        dataCor <- Try(inCor.readObject().asInstanceOf[SerializableMLibClassifier])
      } yield (dataLearnt, dataCor) ) match {
        case Success((data, cor)) =>
          assert(data.classes === cor.classes)
          val rfModel_new = data.model.stages(2).asInstanceOf[RandomForestClassificationModel]
          val rfModel_one = cor.model.stages(2).asInstanceOf[RandomForestClassificationModel]
          assert(rfModel_new.numClasses === rfModel_one.numClasses)
          assert(rfModel_new.numFeatures === rfModel_one.numFeatures)
          assert(rfModel_new.treeWeights === rfModel_one.treeWeights)
//          assert(rfModel_new.numTrees === rfModel_one.numTrees)

          assert(rfModel_new.totalNumNodes === rfModel_one.totalNumNodes)
          assert(rfModel_new.featureImportances === rfModel_one.featureImportances)
          assert(data.featureExtractors === cor.featureExtractors)

        case Failure(err) =>
          throw new Exception(err.getMessage)
      }

    } finally {
      deleteAllModels
      deleteAllDatasets
      assertClose()
    }
  })

  test("Model.rf from older versions cannot be read in") (new TestServer {
    try {
      // pre-computed model with default spark config
      val oldFile = Paths.get(helperDir, "default-model.rf").toFile

      (for {
        inCor <- Try( new ObjectInputStreamWithCustomClassLoader(new FileInputStream(oldFile)))
        dataCor <- Try(inCor.readObject().asInstanceOf[SerializableMLibClassifier])
      } yield dataCor ) match {
        case Success(cor) =>
          fail("Old .rf file has been read which should not happen!")

        case Failure(err) =>
          succeed
      }

    } finally {
      deleteAllModels
      deleteAllDatasets
      assertClose()
    }
  })

  test("POST /v1.0/model/:id/predict/:id returns successfully") (new TestServer {
    try {
      val PollTime = 1000
      val PollIterations = 60

      val (model, ds) = trainDefault()

      // now just make sure it completes...
      val trained = pollModelState(model, PollIterations, PollTime)
      val state = concurrent.Await.result(trained, PollTime * PollIterations * 2 seconds)

      assert(state === Training.Status.COMPLETE)

      // now make a prediction
      val request = RequestBuilder()
        .url(s.fullUrl(s"/$APIVersion/model/${model.id}/predict/${ds.id}"))
        .addHeader("Content-Type", "application/json")
        .buildPost(Buf.Utf8(""))

      val response = Await.result(client(request))

      assert(response.status === Status.Ok)

    } finally {
      deleteAllModels
      deleteAllDatasets
      assertClose()
    }
  })

  test("POST /v1.0/model/:id/predict/:id using character dist and entropy returns successfully") (new TestServer {
    try {
      val PollTime = 1000
      val PollIterations = 60

      val (model, ds) = trainDefault(features = charDistFeatures)

      // now just make sure it completes...
      val trained = pollModelState(model, PollIterations, PollTime)
      val state = concurrent.Await.result(trained, PollTime * PollIterations * 2 seconds)

      assert(state === Training.Status.COMPLETE)

      // now make a prediction
      val request = RequestBuilder()
        .url(s.fullUrl(s"/$APIVersion/model/${model.id}/predict/${ds.id}"))
        .addHeader("Content-Type", "application/json")
        .buildPost(Buf.Utf8(""))

      val response = Await.result(client(request))

      assert(response.status === Status.Ok)

    } finally {
      deleteAllModels
      deleteAllDatasets
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
      deleteAllModels
      deleteAllDatasets
      assertClose()
    }
  })

  test("POST /v1.0/model/:id/predict/:id returns predictions with validation > 0.9") (new TestServer {
    try {
      val PollTime = 1000
      val PollIterations = 60

      // ResampleToMean gives worse performance than upsampletomax or noresampling!
      val (model, ds) = trainDefault(features = fullFeatures, resamplingStrategy = "UpsampleToMax")

      // now just make sure it completes...
      val trained = pollModelState(model, PollIterations, PollTime)
      val state = concurrent.Await.result(trained, PollTime * PollIterations * 2 seconds)

      assert(state === Training.Status.COMPLETE)

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
      deleteAllModels
      deleteAllDatasets
      assertClose()
    }
  })



  // TODO: prediction is performed again if there are changes to the model
  // TODO: prediction is performed again if there are changes to the dataset

//  TODO: create test for bad parameters
//
//  TODO: create model with incorrect featuresconfig feature names?? incorrect feature names are ignored currently both by API and data integration project

}

