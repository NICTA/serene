///**
//  * Copyright (C) 2015-2016 Data61, Commonwealth Scientific and Industrial Research Organisation (CSIRO).
//  * See the LICENCE.txt file distributed with this work for additional
//  * information regarding copyright ownership.
//  *
//  * Licensed under the Apache License, Version 2.0 (the "License");
//  * you may not use this file except in compliance with the License.
//  * You may obtain a copy of the License at
//  *
//  * http://www.apache.org/licenses/LICENSE-2.0
//  *
//  * Unless required by applicable law or agreed to in writing, software
//  * distributed under the License is distributed on an "AS IS" BASIS,
//  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  * See the License for the specific language governing permissions and
//  * limitations under the License.
//  */
//package au.csiro.data61.core
//
//
//import java.io.{File, FileInputStream, IOException, ObjectInputStream}
//import java.nio.file.{Path, Paths}
//
//import au.csiro.data61.core.api.DatasetAPI._
//import au.csiro.data61.types.ModelTypes.{Model, ModelID}
//import au.csiro.data61.types._
//import au.csiro.data61.core.drivers.{ModelPredictor, ModelTrainer, ObjectInputStreamWithCustomClassLoader}
//import com.twitter.finagle.http.RequestBuilder
//import com.twitter.finagle.http._
//import com.twitter.io.Buf
//import com.twitter.util.{Await, Return, Throw}
//import com.typesafe.scalalogging.LazyLogging
//import org.apache.commons.io.FileUtils
//import org.junit.runner.RunWith
//import org.scalatest.{BeforeAndAfterEach, FunSuite}
//import org.scalatest.junit.JUnitRunner
//import org.scalatest.concurrent._
//
//import scala.concurrent._
//import scala.concurrent.ExecutionContext.Implicits.global
//import scala.concurrent.duration._
//import api._
//import au.csiro.data61.core.storage.{JsonFormats, ModelStorage}
//import au.csiro.data61.matcher.data.{Attribute, DataModel, Metadata}
//import au.csiro.data61.matcher.matcher.features._
//import au.csiro.data61.matcher.matcher.serializable.SerializableMLibClassifier
//import com.twitter.finagle.http
//import org.apache.spark.ml.classification.RandomForestClassificationModel
//
//import language.postfixOps
//import scala.annotation.tailrec
//import scala.concurrent.Future
//import scala.util.{Failure, Random, Success, Try}
//import org.json4s._
//import org.json4s.JsonDSL._
//import org.json4s.jackson.JsonMethods._
//
///**
//  * Tests for the Model REST endpoint API
//  */
//@RunWith(classOf[JUnitRunner])
//class FeatureExtractorSpec extends FunSuite with JsonFormats with BeforeAndAfterEach with Futures with LazyLogging {
//
//  import ModelAPI._
//
//  /**
//    * Deletes all the models from the server. Assumes that
//    * the IDs are stored as positive integers
//    *
//    * @param server Reference to the TestServer used in a single test
//    */
//  def deleteAllModels()(implicit server: TestServer): Unit = {
//    val response = server.get(s"/$APIVersion/model")
//
//    if (response.status == Status.Ok) {
//      val str = response.contentString
//      val regex = "[0-9]+".r
//      val models = regex.findAllIn(str).map(_.toInt)
//      models.foreach { model =>
//        server.delete(s"/$APIVersion/model/$model")
//      }
//    }
//  }
//
//  // we need a dataset server to hold datasets for training...
//  val DataSet = new DatasetRestAPISpec
//  val TypeMap = """{"a":"b", "c":"d"}"""
//  val Resource = DataSet.Resource
//
//  def randomString: String = Random.alphanumeric take 10 mkString
//
//  def defaultFeatures: JObject =
//    ("activeFeatures" -> Seq("num-unique-vals", "prop-unique-vals", "prop-missing-vals" )) ~
//      ("activeFeatureGroups" -> Seq("stats-of-text-length", "prop-instances-per-class-in-knearestneighbours",
//        "mean-character-cosine-similarity-from-class-examples",
//        "min-editdistance-from-class-examples",
//        "min-wordnet-jcn-distance-from-class-examples",
//        "min-wordnet-lin-distance-from-class-examples")) ~
//      ("featureExtractorParams" -> Seq(
//        ("name" -> "prop-instances-per-class-in-knearestneighbours") ~
//          ("num-neighbours" -> 5),
//        ("name" -> "min-wordnet-jcn-distance-from-class-examples") ~
//          ("max-comparisons-per-class" -> 5),
//        ("name" -> "min-wordnet-lin-distance-from-class-examples") ~
//          ("max-comparisons-per-class" -> 5)
//      ))
//
//
//  def defaultCostMatrix: JArray =
//    JArray(List(JArray(List(1,0,0)), JArray(List(0,1,0)), JArray(List(0,0,1))))
//
//  def defaultDataSet: String = getClass.getResource("/homeseekers.csv").getPath
//
//  // default classes for the homeseekers dataset
//  def defaultClasses: List[String] = List(
//    "unknown",
//    "year_built",
//    "address",
//    "bathrooms",
//    "bedrooms",
//    "email",
//    "fireplace",
//    "firm_name",
//    "garage",
//    "heating",
//    "house_description",
//    "levels",
//    "mls",
//    "phone",
//    "price",
//    "size",
//    "type"
//  )
//
//  // index labels for the default homeseekers dataset
//  def defaultLabels: Map[Int, String] =
//  Map(
//    4  -> "address",
//    5  -> "firm_name",
//    7  -> "email",
//    9  -> "price",
//    10 -> "type",
//    11 -> "mls",
//    12 -> "levels",
//    14 -> "phone",
//    18 -> "phone",
//    19 -> "year_built",
//    21 -> "garage",
//    24 -> "fireplace",
//    25 -> "bathrooms",
//    27 -> "size",
//    29 -> "house_description",
//    31 -> "phone",
//    30 -> "heating",
//    32 -> "bedrooms"
//  )
//
//  //  val helperDir = Paths.get("src", "test", "resources", "helper").toFile.getAbsolutePath // location for sample files
//  val helperDir = getClass.getResource("/helper").getPath
//  //  Paths.get("src", "test", "resources", "helper").toFile.getAbsolutePath // location for sample files
//
//  def copySampleDatasets(): Unit = {
//    // copy sample dataset to Config.DatasetStorageDir
//    if (!Paths.get(Serene.config.datasetStorageDir).toFile.exists) { // create dataset storage dir
//      Paths.get(Serene.config.datasetStorageDir).toFile.mkdirs}
//    val dsDir = Paths.get(helperDir, "sample.datasets").toFile // directory to copy from
//    FileUtils.copyDirectory(dsDir,                    // copy sample dataset
//      Paths.get(Serene.config.datasetStorageDir).toFile)
//  }
//
//  def copySampleModels(): Unit = {
//    // copy sample model to Config.ModelStorageDir
//    if (!Paths.get(Serene.config.modelStorageDir).toFile.exists) { // create model storage dir
//      Paths.get(Serene.config.modelStorageDir).toFile.mkdirs}
//    val mDir = Paths.get(helperDir, "sample.models").toFile // directory to copy from
//    FileUtils.copyDirectory(mDir,                    // copy sample model
//      Paths.get(Serene.config.modelStorageDir).toFile)
//  }
//
//  def copySampleFiles(): Unit = {
//    copySampleDatasets()
//    copySampleModels()
//  }
//
//  /**
//    * Builds a standard POST request object from a json object.
//    *
//    * @param json
//    * @param url
//    * @return
//    */
//  def postRequest(json: JObject, url: String = s"/$APIVersion/model")(implicit s: TestServer): Request = {
//    RequestBuilder()
//      .url(s.fullUrl(url))
//      .addHeader("Content-Type", "application/json")
//      .buildPost(Buf.Utf8(compact(render(json))))
//  }
//
//  /**
//    * Posts a request to build a model, then returns the Model object it created
//    * wrapped in a Try.
//    *
//    * @param classes The model request object
//    * @param description Optional description
//    * @param labelDataMap Optional map for column labels
//    * @param numBags Optional integer numBags
//    * @param bagSize OPtional integer bagSize
//    * @return Model that was constructed
//    */
//  def createModel(classes: List[String],
//                  description: Option[String] = None,
//                  labelDataMap: Option[Map[String, String]] = None,
//                  resamplingStrategy: String = "ResampleToMean",
//                  numBags: Option[Int] = None,
//                  bagSize: Option[Int] = None)(implicit s: TestServer): Try[Model] = {
//
//    Try {
//
//      val json =
//        ("description" -> description.getOrElse("unknown")) ~
//          ("modelType" -> "randomForest") ~
//          ("classes" -> classes) ~
//          ("features" -> defaultFeatures) ~
//          ("costMatrix" -> defaultCostMatrix) ~
//          ("resamplingStrategy" -> resamplingStrategy) ~
//          ("numBags" -> numBags) ~
//          ("bagSize" -> bagSize)
//
//      // add the labelData if available...
//      val labelJson = labelDataMap.map { m =>
//        json ~ ("labelData" -> m)
//      }.getOrElse(json)
//
//      val req = postRequest(labelJson)
//
//      val response = Await.result(s.client(req))
//
//      parse(response.contentString).extract[Model]
//    }
//  }
//
//  /**
//    * createDataSet creates a single simple dataset from the medium.csv file
//    * in the DataSet test spec.
//    *
//    * @param server The server object
//    * @return List of column IDs...
//    */
//  def createDataSet(implicit server: TestServer): DataSet = {
//    // first we add a dataset...
//    DataSet.createDataset(server, defaultDataSet, TypeMap, "homeseekers") match {
//      case Success(ds) =>
//        ds
//      case _ =>
//        throw new Exception("Failed to create dataset")
//    }
//  }
//
//  /**
//    * createLabelMap creates the default labels from the
//    * created dataset...
//    *
//    * @param ds The dataset that was created
//    * @return
//    */
//  def createLabelMap(ds: DataSet): Map[String, String] = {
//    // next grab the columns
//    val cols = ds.columns.map(_.id.toString)
//
//    // now we create the colId -> labelMap
//    defaultLabels.map { case (i, v) =>
//      cols(i) -> v
//    }
//  }
//
//  /**
//    * pollModelState
//    *
//    * @param model
//    * @param pollIterations
//    * @param pollTime
//    * @param s
//    * @return
//    */
//  def pollModelState(model: Model, pollIterations: Int, pollTime: Int)(implicit s: TestServer): Future[ModelTypes.Status] = {
//    Future {
//
//      def state(): ModelTypes.Status = {
//        Thread.sleep(pollTime)
//        // build a request to get the model...
//        val response = s.get(s"/$APIVersion/model/${model.id}")
//        if (response.status != Status.Ok) {
//          throw new Exception("Failed to retrieve model state")
//        }
//        // ensure that the data is correct...
//        val m = parse(response.contentString).extract[Model]
//
//        m.state.status
//      }
//
//      @tailrec
//      def rState(loops: Int): ModelTypes.Status = {
//        state() match {
//          case s@ModelTypes.Status.COMPLETE =>
//            s
//          case s@ModelTypes.Status.ERROR =>
//            s
//          case _ if loops < 0 =>
//            throw new Exception("Training timeout")
//          case _ =>
//            rState(loops - 1)
//        }
//      }
//
//      rState(pollIterations)
//    }
//  }
//
//  /**
//    * This helper function will start the training...
//    *
//    * @param server
//    * @return
//    */
//  def trainDefault(resamplingStrategy: String = "ResampleToMean",
//                   numBags: Option[Int] = None,
//                   bagSize: Option[Int] = None)(implicit server: TestServer): (Model, DataSet) = {
//    val TestStr = randomString
//
//    // first we add a simple dataset
//    val ds = createDataSet(server)
//    val labelMap = createLabelMap(ds)
//
//    // next we train the dataset
//    createModel(defaultClasses, Some(TestStr), Some(labelMap), resamplingStrategy, numBags, bagSize) match {
//
//      case Success(model) =>
//
//        val request = RequestBuilder()
//          .url(server.fullUrl(s"/$APIVersion/model/${model.id}/train"))
//          .addHeader("Content-Type", "application/json")
//          .buildPost(Buf.Utf8(""))
//
//        // send the request and make sure it executes
//        val response = Await.result(server.client(request))
//
//        assert(response.status === Status.Accepted)
//        assert(response.contentString.isEmpty)
//
//        (model, ds)
//      case Failure(err) =>
//        throw new Exception("Failed to create test resource")
//    }
//  }
//
//
//  //=========================Tests==============================================
//
////  test("JSON serialization of featureExtractors") (new TestServer {
////    try {
////      val TestStr = randomString
////
////      // first we add a simple dataset
////      val ds = createDataSet
////      val labelMap = createLabelMap(ds)
////
////      // next we train the dataset
////      val model: Model = createModel(defaultClasses, Some(TestStr), Some(labelMap), "ResampleToMean", None, None).get
////      println("model created")
////
////      val sModel: SerializableMLibClassifier = ModelTrainer.train(model.id).get
////      println("model trained")
////
////      val groupFeatureExtractors: List[NamedGroupFeatureExtractor] =
////        sModel.featureExtractors.flatMap {
////          case x: GroupFeatureExtractor =>
////            List(NamedGroupFeatureExtractor(x.getGroupName, x))
////          case _ => None
////        }
////
////      val singleFeatureExtractors: List[SingleFeatureExtractor] = sModel.featureExtractors
////        .flatMap {
////          case x: SingleFeatureExtractor => List(x)
////          case _ => None
////      }
////
////      val jGroup = Extraction.decompose(groupFeatureExtractors)
////      val jSingle = Extraction.decompose(singleFeatureExtractors)
////
////      val singleFeat = jSingle.extract[List[SingleFeatureExtractor]]
////      val groupFeat = jGroup.extract[List[NamedGroupFeatureExtractor]]
////
////      assert(singleFeat === singleFeatureExtractors)
////      assert(groupFeat === groupFeatureExtractors)
////
////      println(s"!!!!singleFeatures: ${singleFeatureExtractors.map(_.getFeatureName)}")
////      println(s"!!!!groupFeatures: ${groupFeatureExtractors.map(_.name)}")
////      val modelFeatureExtractors = ModelFeatureExtractors(sModel.featureExtractors)
////      val jModelFExtr = Extraction.decompose(modelFeatureExtractors)
////
////      println("extracting...")
////      val modelFeatExtr2 = jModelFExtr.extract[ModelFeatureExtractors]
////
////      assert(modelFeatureExtractors === modelFeatExtr2)
////
////    } finally {
////      deleteAllModels()
////      DataSet.deleteAllDataSets()
////      assertClose()
////    }
////  })
//
//  test("feature extractor test")  {
//    val fe = CharDistFeatureExtractor()
//    lazy val dm: DataModel = new DataModel("", None, None, Some(List(attr)))
//    lazy val attr: Attribute = Attribute("",
//      Some(Metadata("", "")),
////      None,
//      List("akf;asf","akjflsafz","akjfalsdfk", "bxv"),
//      Some(dm))
//
////    println(attr.metadata.exists(_.nonEmpty))
//
//    println(s"first: ${attr.metadata.map({x => true}).getOrElse(false)}")
//
//    println(s"second: ${attr.metadata.nonEmpty}")
//    val preprocessedAttribute = DataPreprocessor().preprocess(attr)
//
//    println("preprocessed")
//    val v1 = preprocessedAttribute.preprocessedDataMap
//      .get("normalised-char-frequency-vector").get
//    println(v1)
//    println("computed")
//    val v2 = fe.computeFeatures(preprocessedAttribute)
//    println(v2)
//
////    assert( v1 === v2)
//  }
//
//}
//
