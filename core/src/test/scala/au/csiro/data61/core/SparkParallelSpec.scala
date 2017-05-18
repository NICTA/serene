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


import java.nio.file.Paths

import au.csiro.data61.core.drivers.{ModelPredictor, ModelTrainer}
import au.csiro.data61.types._
import api._
import au.csiro.data61.core.storage.{JsonFormats, ModelStorage}
import au.csiro.data61.matcher.matcher.serializable.SerializableMLibClassifier
import au.csiro.data61.types.ModelTypes.Model
import org.apache.spark.ml.classification.RandomForestClassificationModel

import language.postfixOps
import scala.util.{Failure, Random, Success, Try}
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._
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


/**
  * Tests for model training and prediction using spark
  */
@RunWith(classOf[JUnitRunner])
class SparkParallelSpec extends FunSuite with JsonFormats with BeforeAndAfterEach with Futures with LazyLogging {

  import ModelAPI._

  implicit val version = APIVersion

  // we need a dataset server to hold datasets for training...
  val DataSet = new DatasetRestAPISpec
  val TypeMap = """{"a":"b", "c":"d"}"""
  val Resource = DataSet.Resource

  def randomString: String = Random.alphanumeric take 10 mkString

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

  def defaultFeatures: JObject =
      ("activeFeatures" -> Seq("num-unique-vals", "prop-unique-vals", "prop-missing-vals" )) ~
      ("activeFeatureGroups" -> Seq("stats-of-text-length", "prop-instances-per-class-in-knearestneighbours")) ~
      ("featureExtractorParams" -> Seq(
        ("name" -> "prop-instances-per-class-in-knearestneighbours") ~
          ("num-neighbours" -> 5)))


  def defaultCostMatrix: JArray =
    JArray(List(JArray(List(1,0,0)), JArray(List(0,1,0)), JArray(List(0,0,1))))

  def defaultDataSet: String = getClass.getResource("/homeseekers.csv").getPath
  def employeeDataSet: String = getClass.getResource("/Employees.csv").getPath

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
    if (!Paths.get(Serene.config.storageDirs.dataset).toFile.exists) { // create dataset storage dir
      Paths.get(Serene.config.storageDirs.dataset).toFile.mkdirs}
    val dsDir = Paths.get(helperDir, "sample.datasets").toFile // directory to copy from
    FileUtils.copyDirectory(dsDir,                    // copy sample dataset
      Paths.get(Serene.config.storageDirs.dataset).toFile)
  }

  def copySampleModels(): Unit = {
    // copy sample model to Config.ModelStorageDir
    if (!Paths.get(Serene.config.storageDirs.model).toFile.exists) { // create model storage dir
      Paths.get(Serene.config.storageDirs.model).toFile.mkdirs}
    val mDir = Paths.get(helperDir, "sample.models").toFile // directory to copy from
    FileUtils.copyDirectory(mDir,                    // copy sample model
      Paths.get(Serene.config.storageDirs.model).toFile)
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
  def createDataSet(implicit server: TestServer): DataSet = {
    // first we add a dataset...
    server.createDataset(Paths.get(defaultDataSet).toFile, "homeseekers", TypeMap) match {
      case Success(ds) =>
        ds
      case _ =>
        throw new Exception("Failed to create dataset")
    }
  }

  /**
    * createDataSet creates a single simple dataset from the medium.csv file
    * in the DataSet test spec.
    *
    * @param server The server object
    * @return List of column IDs...
    */
  def createSimpleDataSet(implicit server: TestServer): DataSet = {
    // first we add a dataset...
    server.createDataset(Paths.get(employeeDataSet).toFile, "Employees", TypeMap) match {
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
    * This helper function will start the training...
    *
    * @param resamplingStrategy string
    * @param numBags optional
    * @param bagSize optional
    * @param server implicit
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


  test("same model with default features and no resampling in parallel is learnt") (new TestServer {
    try {

      // first we add a simple dataset
      val ds = createDataSet
      val labelMap = createLabelMap(ds)
      // next we train the dataset
      val TestStr = randomString
      val model: Model = createModel(defaultClasses, Some(TestStr),
        Some(labelMap), "NoResampling", None, None, defaultFeatures).get

      // default config
      Serene.config = Config(args = Array.empty[String], Some(true, 1))
      val sModel_default: SerializableMLibClassifier = ModelTrainer.train(model.id).get
      ModelStorage.addModel(model.id, sModel_default)

      // no parallel feature extraction and default num workers for spark
      Serene.config = Config(args = Array.empty[String], Some(true, 8))
      val sModel_no: SerializableMLibClassifier = ModelTrainer.train(model.id).get

      val rfModel_default = sModel_default.model.stages(2).asInstanceOf[RandomForestClassificationModel]
      val rfModel_no = sModel_no.model.stages(2).asInstanceOf[RandomForestClassificationModel]

      assert(sModel_default.classes === sModel_no.classes)
      assert(sModel_default.featureExtractors === sModel_no.featureExtractors)
      assert(rfModel_default.numClasses === rfModel_no.numClasses)
      assert(rfModel_default.numFeatures === rfModel_no.numFeatures)
      assert(rfModel_default.treeWeights === rfModel_no.treeWeights)
      assert(rfModel_default.totalNumNodes === rfModel_no.totalNumNodes)
      assert(rfModel_default.featureImportances === rfModel_no.featureImportances)

    } finally {
      deleteAllModels
      deleteAllDatasets
      assertClose()
    }

  })

  test("same model with default features and no resampling in parallel and no parallel is learnt") (new TestServer {
    try {

      // first we add a simple dataset
      val ds = createDataSet
      val labelMap = createLabelMap(ds)
      // next we train the dataset
      val TestStr = randomString
      val model: Model = createModel(defaultClasses, Some(TestStr),
        Some(labelMap), "NoResampling", None, None, defaultFeatures).get

      // default config
      Serene.config = Config(args = Array.empty[String], Some(false, 2))
      val sModel_default: SerializableMLibClassifier = ModelTrainer.train(model.id).get

      // no parallel feature extraction and default num workers for spark
      Serene.config = Config(args = Array.empty[String], Some(true, 8))
      val sModel_no: SerializableMLibClassifier = ModelTrainer.train(model.id).get

      val rfModel_default = sModel_default.model.stages(2).asInstanceOf[RandomForestClassificationModel]
      val rfModel_no = sModel_no.model.stages(2).asInstanceOf[RandomForestClassificationModel]

      assert(sModel_default.classes === sModel_no.classes)
      assert(sModel_default.featureExtractors === sModel_no.featureExtractors)
      assert(rfModel_default.numClasses === rfModel_no.numClasses)
      assert(rfModel_default.numFeatures === rfModel_no.numFeatures)
      assert(rfModel_default.treeWeights === rfModel_no.treeWeights)
      assert(rfModel_default.totalNumNodes === rfModel_no.totalNumNodes)
      assert(rfModel_default.featureImportances === rfModel_no.featureImportances)

    } finally {
      deleteAllModels
      deleteAllDatasets
      assertClose()
    }

  })

  test("train and predict and check accuracy") (new TestServer {
    try {
      val TestStr = randomString

      // first we add a simple dataset
      val ds = createDataSet
      val labelMap = createLabelMap(ds)

      // next we train the dataset
      val model: Model = createModel(defaultClasses, Some(TestStr), Some(labelMap), "NoResampling", None, None).get

      val sModel: SerializableMLibClassifier = ModelTrainer.train(model.id).get

      ModelPredictor.runPrediction(model.id, ds.path, sModel, ds.id) match {
        case Some(dsPrediction) =>
          val trueLabels = createLabelMap(ds)
                  .toList
                  .sortBy(_._1)

          // these are the labels that were predicted
          val testLabels = dsPrediction
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
        case _ =>
          fail("Prediction failed!!!")
      }
    } finally {
      deleteAllModels
      deleteAllDatasets
      assertClose()
    }
  })

  test("train and predict with bagging") (new TestServer {
    try {
      val TestStr = randomString

      // first we add a simple dataset
      val ds = createDataSet
      val labelMap = createLabelMap(ds)

      // next we train the dataset
      val model: Model = createModel(defaultClasses, Some(TestStr), Some(labelMap), "Bagging", Some(50), Some(50)).get

      val sModel: SerializableMLibClassifier = ModelTrainer.train(model.id).get

      ModelPredictor.runPrediction(model.id, ds.path, sModel, ds.id) match {
        case Some(dsPrediction) =>
          val trueLabels = createLabelMap(ds)
            .toList
            .sortBy(_._1)

          // these are the labels that were predicted
          val testLabels = dsPrediction
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

          println("TOOOOOOOOTAL")
          println(total)
          println("TOOOOOOOOTAL")
          assert(total > 0.9)
        case _ =>
          fail("Prediction failed!!!")
      }
    } finally {
      deleteAllModels
      deleteAllDatasets
      assertClose()
    }
  })

  test("check features calculated when bagging") (new TestServer {
    try {
      val TestStr = randomString

      // first we add a simple dataset
      val ds = createDataSet
      val labelMap = createLabelMap(ds)

      // next we train the dataset
      val model: Model = createModel(defaultClasses, Some(TestStr), Some(labelMap), "NoResampling", Some(50), Some(50)).get

      val sModel: SerializableMLibClassifier = ModelTrainer.train(model.id).get

      val ds2 = createSimpleDataSet
      val derivedFeatureFile = ModelPredictor.predictionsPath(model.id, ds2.id)
      val predsObject = ModelPredictor.modelPrediction(model.id, ds2.path, sModel, derivedFeatureFile)

      // TODO: check that prop-alpha-chars for DOB is 0

    } finally {
      deleteAllModels
      deleteAllDatasets
      assertClose()
    }
  })

}

