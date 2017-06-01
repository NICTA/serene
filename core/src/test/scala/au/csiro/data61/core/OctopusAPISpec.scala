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

import java.io.FileInputStream
import java.nio.file.Paths
import java.io.File

import au.csiro.data61.core.api._
import au.csiro.data61.core.drivers.Generic._
import au.csiro.data61.core.drivers.{ModelInterface, OctopusInterface}
import au.csiro.data61.core.storage._
import au.csiro.data61.types.ModelTypes.Model
import au.csiro.data61.types.ModelingProperties.ConfidenceWeightShouldBeInRange
import au.csiro.data61.types.SamplingStrategy.{NO_RESAMPLING, RESAMPLE_TO_MEAN}
import au.csiro.data61.types.SsdTypes._
import au.csiro.data61.types._
import com.twitter.finagle.http.{RequestBuilder, _}
import com.twitter.io.Buf
import com.twitter.io.Buf.ByteArray
import com.twitter.util.Await
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FileUtils
import org.joda.time.DateTime
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization._
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import scala.annotation._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Random, Success, Try}


/**
  * Tests for the OctopusAPI endpoints
  */

class OctopusAPISpec extends FunSuite with JsonFormats with BeforeAndAfterEach with LazyLogging {

  import OctopusAPI._

  implicit val version = APIVersion

//  override def afterEach(): Unit = {
//    SsdStorage.removeAll()
//    OctopusStorage.removeAll()
//    DatasetStorage.removeAll()
//    OwlStorage.removeAll()
//    ModelStorage.removeAll()
//  }

  /**
    * Clean all resources from the server
    *
    * @param server Reference to the TestServer used in a single test
    */
  def deleteOctopi()(implicit server: TestServer): Unit = {
    val response = server.get(s"/$APIVersion/octopus")

    if (response.status == Status.Ok) {
      val str = response.contentString
      val regex = "[0-9]+".r
      val models = regex.findAllIn(str).map(_.toInt)
      models.foreach { model =>
        server.delete(s"/$APIVersion/octopus/$model")
      }
    }

    SsdStorage.removeAll()
    OwlStorage.removeAll()
    server.deleteAllDatasets
  }

  val businessSsdID = 0
  val exampleOwlID = 1

  def setUp(): Unit = {
    copySampleDatasets() // copy csv files for getCities and businessInfo
    SsdStorage.add(businessSSD.id, businessSSD) // add businessInfo ssd
    OwlStorage.add(exampleOwl.id, exampleOwl)  // add sample ontology
    // write owl file
    Try{
      val stream = new FileInputStream(exampleOntolPath.toFile)
      OwlStorage.writeOwlDocument(exampleOwlID, stream)
    }

  }

  /**
    * Builds a standard POST request object from a json object for Octopus endpoint.
    *
    * @param json
    * @param url
    * @return
    */
  def postRequest(json: JObject, url: String = s"/$APIVersion/octopus")(implicit s: TestServer): Request = {
    RequestBuilder()
      .url(s.fullUrl(url))
      .addHeader("Content-Type", "application/json")
      .buildPost(Buf.Utf8(compact(render(json))))
  }

  val ssdDir = getClass.getResource("/ssd").getPath
  val owlDir = getClass.getResource("/owl").getPath

  def readSSD(ssdPath: String): Ssd = {
    Try {
      val stream = new FileInputStream(Paths.get(ssdPath).toFile)
      parse(stream).extract[Ssd]
    } match {
      case Success(ssd) =>
        ssd
      case Failure(err) =>
        fail(err.getMessage)
    }
  }

  val exampleOntolPath = Paths.get(owlDir,"dataintegration_report_ontology.owl")
  val exampleOwl = Owl(id = exampleOwlID, name = exampleOntolPath.toString, format = OwlDocumentFormat.Turtle,
    description = "sample", dateCreated = DateTime.now, dateModified = DateTime.now)

  val partialSSD: Ssd = readSSD(Paths.get(ssdDir,"partial_model.ssd").toString)
  val veryPartialSSD: Ssd = readSSD(Paths.get(ssdDir,"partial_model2.ssd").toString)
  val emptyCitiesSSD: Ssd = readSSD(Paths.get(ssdDir,"empty_getCities.ssd").toString)
  val emptySSD: Ssd = readSSD(Paths.get(ssdDir,"empty_model.ssd").toString)
  val businessSSD: Ssd = readSSD(Paths.get(ssdDir,"businessInfo.ssd").toString)

  val inconsistentSsd = Paths.get(ssdDir, "inconsistent_cities.ssd").toFile

  def defaultFeatures: JObject =
    ("activeFeatures" -> Seq("num-unique-vals", "prop-unique-vals", "prop-missing-vals",
      "ratio-alpha-chars", "prop-numerical-chars",
      "prop-whitespace-chars", "prop-entries-with-at-sign",
      "prop-entries-with-hyphen", "prop-entries-with-paren",
      "prop-entries-with-currency-symbol", "mean-commas-per-entry",
      "mean-forward-slashes-per-entry",
      "prop-range-format", "is-discrete", "entropy-for-discrete-values")) ~
      ("activeFeatureGroups" -> Seq("stats-of-text-length", "prop-instances-per-class-in-knearestneighbours")) ~
      ("featureExtractorParams" -> Seq(
        ("name" -> "prop-instances-per-class-in-knearestneighbours") ~
          ("num-neighbours" -> 5)))

  def someModelingProps: JObject =
    ("addOntologyPaths" -> true) ~
      ("topkSteinerTrees" -> 5)



  def randomString: String = Random.alphanumeric take 10 mkString

  val helperDir = getClass.getResource("/helper").getPath
  val sampleDsDir = getClass.getResource("/sample.datasets").getPath
  val datasetMap = Map("businessInfo" -> 767956483, "getCities" -> 696167703)
  val businessDsPath = Paths.get(sampleDsDir,
    datasetMap("businessInfo").toString, datasetMap("businessInfo").toString + ".json")
  val citiesDsPath = Paths.get(sampleDsDir,
    datasetMap("getCities").toString, datasetMap("getCities").toString + ".json")


  /**
    * Manually add datasets to the storage layer since SSDs have hard-coded mappings for columns.
    */
  def copySampleDatasets(): Unit = {
    // copy sample dataset to Config.DatasetStorageDir
    if (!Paths.get(Serene.config.storageDirs.dataset).toFile.exists) { // create dataset storage dir
      Paths.get(Serene.config.storageDirs.dataset).toFile.mkdirs}
    val dsDir = Paths.get(sampleDsDir).toFile // directory to copy from
    FileUtils.copyDirectory(dsDir,                    // copy sample dataset
      Paths.get(Serene.config.storageDirs.dataset).toFile)

    // adding datasets explicitly to the storage
    val businessDS: DataSet = Try {
      val stream = new FileInputStream(businessDsPath.toFile)
      parse(stream).extract[DataSet]
    } match {
      case Success(ds) => ds
      case Failure(err) => fail(err.getMessage)
    }
    val citiesDS: DataSet = Try {
      val stream = new FileInputStream(citiesDsPath.toFile)
      parse(stream).extract[DataSet]
    } match {
      case Success(ds) => ds
      case Failure(err) => fail(err.getMessage)
    }

    DatasetStorage.add(businessDS.id, businessDS)
    DatasetStorage.add(citiesDS.id, citiesDS)
  }

  /**
    * pollOctopusState
    *
    * @param model
    * @param pollIterations
    * @param pollTime
    * @param s
    * @return
    */
  def pollOctopusState(model: Octopus, pollIterations: Int, pollTime: Int)(implicit s: TestServer)
  : Future[Training.Status] = {
    Future {

      def state(): Training.Status = {
        Thread.sleep(pollTime)
        // build a request to get the model...
        val response = s.get(s"/$APIVersion/octopus/${model.id}")
        if (response.status != Status.Ok) {
          throw new Exception("Failed to retrieve model state")
        }
        // ensure that the data is correct...
        val m = parse(response.contentString).extract[Octopus]

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
    * Creates default octopus
    *
    * @param s
    * @return
    */
  def createOctopus()(implicit s: TestServer): Octopus = {
    setUp()

    val json = "ssds" -> List(businessSsdID)

    val request = postRequest(json)
    val response = Await.result(s.client(request))
    assert(response.status === Status.Ok)

    // created octopus
    parse(response.contentString).extract[Octopus]
  }

  def trainOctopus()(implicit s: TestServer): Octopus = {

    val octopus = createOctopus()

    val req = postRequest(json = JObject(), url = s"/$APIVersion/octopus/${octopus.id}/train")
    // send the request and make sure it executes
    val resp = Await.result(s.client(req))

    assert(resp.status === Status.Accepted)
    assert(resp.contentString.isEmpty)

    octopus
  }

  //=========================Tests==============================================
  test("GET /v1.0/octopus responds Ok(200)") (new TestServer {
    try {
      val response = get(s"/$APIVersion/octopus")
      assert(response.contentType === Some(JsonHeader))
      assert(response.status === Status.Ok)
      assert(response.contentString.nonEmpty)
      assert(Try { parse(response.contentString).extract[List[OctopusID]] }.isSuccess)
    } finally {
      deleteOctopi()
      assertClose()
    }
  })

  test("GET /v1.0/octopus/asdf responds NotFound") (new TestServer {
    try {
      val response = get(s"/$APIVersion/octopus/asdf")
      assert(response.status === Status.NotFound)

    } finally {
      deleteOctopi()
      assertClose()
    }
  })

  test("POST /v1.0/octopus responds BadRequest") (new TestServer {
    try {
      val TestStr = randomString
      val randomInt = genID
      val dummySeqInt = List(1, 2, 3)

      val json =
      ("description" -> TestStr) ~
        ("name" -> "very fancy name here") ~
        ("modelType" -> "randomForest") ~
        ("features" -> defaultFeatures) ~
        ("resamplingStrategy" -> "NoResampling") ~
        ("numBags" -> randomInt) ~
        ("bagSize" -> randomInt) ~
        ("ssds" -> dummySeqInt) ~
        ("ontologies" -> dummySeqInt) ~
        ("modelingProps" -> TestStr)

      val request = postRequest(json)

      val response = Await.result(client(request))

      assert(response.contentType === Some(JsonHeader))
      assert(response.status === Status.BadRequest)
      assert(!response.contentString.isEmpty)

    } finally {
      deleteOctopi()
      assertClose()
    }
  })

  test("POST /v1.0/octopus responds Ok")(new TestServer {
    try {
      val TestStr = randomString
      val randomInt = genID
      setUp()

      val json =
        ("description" -> TestStr) ~
          ("name" -> TestStr) ~
          ("modelType" -> "randomForest") ~
          ("features" -> defaultFeatures) ~
          ("resamplingStrategy" -> "NoResampling") ~
          ("numBags" -> randomInt) ~
          ("bagSize" -> randomInt) ~
          ("ssds" -> List(businessSsdID)) ~
          ("ontologies" -> List(exampleOwlID)) ~
          ("modelingProps" -> TestStr)

      val request = postRequest(json)

      val response = Await.result(client(request))

      assert(response.contentType === Some(JsonHeader))
      assert(response.status === Status.Ok)
      assert(!response.contentString.isEmpty)

      val octopus = parse(response.contentString).extract[Octopus]
      assert(octopus.description === TestStr)
      assert(octopus.name === TestStr)
      assert(octopus.state.status === Training.Status.UNTRAINED)
      assert(octopus.ssds === List(businessSsdID))
      assert(octopus.ontologies === List(exampleOwlID))

      assert(ModelStorage.get(octopus.lobsterID).nonEmpty)
      val model = ModelStorage.get(octopus.lobsterID).get
      assert(model.modelType === ModelType.RANDOM_FOREST)
      assert(model.numBags === randomInt)
      assert(model.bagSize === randomInt)
      assert(model.state.status === Training.Status.UNTRAINED)
      assert(model.resamplingStrategy === NO_RESAMPLING)

    } finally {
      deleteOctopi()
      assertClose()
    }
  })

  test("POST /v1.0/octopus with modeling properties responds Ok")(new TestServer {
    try {
      val TestStr = randomString
      val randomInt = genID
      setUp()

      val json =
        ("description" -> TestStr) ~
          ("name" -> TestStr) ~
          ("modelType" -> "randomForest") ~
          ("features" -> defaultFeatures) ~
          ("resamplingStrategy" -> "NoResampling") ~
          ("numBags" -> randomInt) ~
          ("bagSize" -> randomInt) ~
          ("ssds" -> List(businessSsdID)) ~
          ("ontologies" -> List(exampleOwlID)) ~
          ("modelingProps" -> someModelingProps)

      val request = postRequest(json)

      val response = Await.result(client(request))

      assert(response.contentType === Some(JsonHeader))
      assert(response.status === Status.Ok)
      assert(!response.contentString.isEmpty)

      val octopus = parse(response.contentString).extract[Octopus]
      assert(octopus.description === TestStr)
      assert(octopus.name === TestStr)
      assert(octopus.state.status === Training.Status.UNTRAINED)
      assert(octopus.ssds === List(businessSsdID))
      assert(octopus.ontologies === List(exampleOwlID))
      // check modeling properties
      assert(octopus.modelingProps.addOntologyPaths)
      assert(octopus.modelingProps.topkSteinerTrees === 5)
      assert(octopus.modelingProps.compatibleProperties)
      assert(!octopus.modelingProps.ontologyAlignment)
      assert(octopus.modelingProps.mappingBranchingFactor === 50)
      assert(octopus.modelingProps.numCandidateMappings === 10)
      assert(!octopus.modelingProps.multipleSameProperty)
      assert(octopus.modelingProps.confidenceWeight === 1.0)
      assert(octopus.modelingProps.coherenceWeight === 1.0)
      assert(octopus.modelingProps.sizeWeight === 0.5)
      assert(octopus.modelingProps.numSemanticTypes === 4)
      assert(!octopus.modelingProps.thingNode)
      assert(octopus.modelingProps.nodeClosure)
      assert(octopus.modelingProps.propertiesDirect)
      assert(octopus.modelingProps.propertiesIndirect)
      assert(octopus.modelingProps.propertiesSubclass)
      assert(octopus.modelingProps.propertiesWithOnlyDomain)
      assert(octopus.modelingProps.propertiesWithOnlyRange)
      assert(!octopus.modelingProps.propertiesWithoutDomainRange)

      assert(ModelStorage.get(octopus.lobsterID).nonEmpty)
      val model = ModelStorage.get(octopus.lobsterID).get
      assert(model.modelType === ModelType.RANDOM_FOREST)
      assert(model.numBags === randomInt)
      assert(model.bagSize === randomInt)
      assert(model.state.status === Training.Status.UNTRAINED)
      assert(model.resamplingStrategy === NO_RESAMPLING)

    } finally {
      deleteOctopi()
      assertClose()
    }
  })

  test("POST /v1.0/octopus with invalid modeling properties responds BadRequest") (new TestServer {
    try {
      val TestStr = randomString
      val randomInt = genID
      val dummySeqInt = List(1, 2, 3)

      val json =
      ("description" -> TestStr) ~
        ("name" -> "very fancy name here") ~
        ("modelType" -> "randomForest") ~
        ("features" -> defaultFeatures) ~
        ("resamplingStrategy" -> "NoResampling") ~
        ("numBags" -> randomInt) ~
        ("bagSize" -> randomInt) ~
        ("ssds" -> dummySeqInt) ~
        ("ontologies" -> dummySeqInt) ~
        ("modelingProps" -> ("confidenceWeight", 1.1))

      val request = postRequest(json)

      val response = Await.result(client(request))

      assert(response.contentType === Some(JsonHeader))
      assert(response.status === Status.BadRequest)
      assert(response.contentString.contains(ConfidenceWeightShouldBeInRange.message))

    } finally {
      deleteOctopi()
      assertClose()
    }
  })

  test("POST /v1.0/octopus with no ontologies responds Ok")(new TestServer {
    try {
      val TestStr = randomString
      val randomInt = genID
      setUp()

      val json =
        ("description" -> TestStr) ~
          ("name" -> TestStr) ~
          ("modelType" -> "randomForest") ~
          ("features" -> defaultFeatures) ~
          ("resamplingStrategy" -> "NoResampling") ~
          ("numBags" -> randomInt) ~
          ("bagSize" -> randomInt) ~
          ("ssds" -> List(businessSsdID)) ~
          ("modelingProps" -> TestStr)

      val request = postRequest(json)

      val response = Await.result(client(request))

      assert(response.contentType === Some(JsonHeader))
      assert(response.status === Status.Ok)
      assert(!response.contentString.isEmpty)

      val octopus = parse(response.contentString).extract[Octopus]
      assert(octopus.description === TestStr)
      assert(octopus.name === TestStr)
      assert(octopus.state.status === Training.Status.UNTRAINED)
      assert(octopus.ssds === List(businessSsdID))
      assert(octopus.ontologies === List(exampleOwlID))

      assert(ModelStorage.get(octopus.lobsterID).nonEmpty)
      val model = ModelStorage.get(octopus.lobsterID).get
      assert(model.modelType === ModelType.RANDOM_FOREST)
      assert(model.numBags === randomInt)
      assert(model.bagSize === randomInt)
      assert(model.state.status === Training.Status.UNTRAINED)
      assert(model.resamplingStrategy === NO_RESAMPLING)

    } finally {
      deleteOctopi()
      assertClose()
    }
  })

  test("POST /v1.0/octopus with only ssds responds Ok")(new TestServer {
    try {
      setUp()

      val json = "ssds" -> List(businessSsdID)

      val request = postRequest(json)
      val response = Await.result(client(request))

      assert(response.contentType === Some(JsonHeader))
      assert(response.status === Status.Ok)
      assert(!response.contentString.isEmpty)

      val octopus = parse(response.contentString).extract[Octopus]
      assert(octopus.description === OctopusInterface.MissingValue)
      assert(octopus.name === OctopusInterface.MissingValue)
      assert(octopus.state.status === Training.Status.UNTRAINED)
      assert(octopus.ssds === List(businessSsdID))
      assert(octopus.ontologies === List(exampleOwlID))
      assert(octopus.modelingProps === ModelingProperties())

      assert(ModelStorage.get(octopus.lobsterID).nonEmpty)
      val model = ModelStorage.get(octopus.lobsterID).get
      // model will all parameters set to default
      assert(model.modelType === ModelType.RANDOM_FOREST)
      assert(model.description === OctopusInterface.MissingValue)
      assert(model.numBags === ModelTypes.defaultNumBags)
      assert(model.bagSize === ModelTypes.defaultBagSize)
      assert(model.state.status === Training.Status.UNTRAINED)
      assert(model.resamplingStrategy === NO_RESAMPLING)
      assert(model.features === ModelInterface.DefaultFeatures)

    } finally {
      deleteOctopi()
      assertClose()
    }
  })

  test("DELETE octopi")(new TestServer {
    try {
      val TestStr = randomString
      val randomInt = genID
      setUp()

      val json =
        ("description" -> TestStr) ~
          ("name" -> TestStr) ~
          ("modelType" -> "randomForest") ~
          ("features" -> defaultFeatures) ~
          ("resamplingStrategy" -> "NoResampling") ~
          ("numBags" -> randomInt) ~
          ("bagSize" -> randomInt) ~
          ("ssds" -> List(businessSsdID)) ~
          ("ontologies" -> List(exampleOwlID)) ~
          ("modelingProps" -> TestStr)

      val request = postRequest(json)

      val response = Await.result(client(request))

      assert(response.status === Status.Ok)
      assert(!response.contentString.isEmpty)

      val octopus = parse(response.contentString).extract[Octopus]
      deleteOctopi()

      assert(OctopusStorage.keys.isEmpty)
      assert(OctopusStorage.get(octopus.id).isEmpty)
      // the associated schema matcher model should be also deleted
      assert(ModelStorage.get(octopus.lobsterID).isEmpty)

    } finally {
      deleteOctopi()
      assertClose()
    }
  })

  test("DELETE /v1.0/octopus/:id responds Ok(200)") (new TestServer {
    try {
      val octopus = createOctopus()

      // build a request to modify the OCTOPUS...
      val resource = s"/$APIVersion/octopus/${octopus.id}"

      val response = delete(resource)
      assert(response.contentType === Some(JsonHeader))
      assert(response.status === Status.Ok)
      assert(!response.contentString.isEmpty)

      // there should be nothing there, and the response
      // should say so.
      val noResource = get(resource)
      assert(noResource.contentType === Some(JsonHeader))
      assert(noResource.status === Status.NotFound)
      assert(noResource.contentString.nonEmpty)

      // the associated lobster should be also deleted
      val noLobsterResource = get(s"/$APIVersion/model/${octopus.lobsterID}")
      assert(noLobsterResource.contentType === Some(JsonHeader))
      assert(noLobsterResource.status === Status.NotFound)
      assert(noLobsterResource.contentString.nonEmpty)

    } finally {
      deleteOctopi()
      assertClose()
    }
  })

  test("DELETE /v1.0/octopus/1 responds octopus NotFound") (new TestServer {
    try {
       // build a request to delete octopus
      val resource = s"/$APIVersion/octopus/1"

      val response = delete(resource)
      assert(response.contentType === Some(JsonHeader))
      assert(response.status === Status.NotFound)
      assert(response.contentString.nonEmpty)

    } finally {
      deleteOctopi()
      assertClose()
    }
  })


  test("POST /v1.0/octopus/:id octopus update responds Ok(200)") (new TestServer {
    try {

      val octopus = createOctopus()
      val PauseTime = 1000

      Thread.sleep(PauseTime)

      val TestStr = randomString
      val randomInt = genID
      val json =
        ("description" -> TestStr) ~
          ("name" -> TestStr) ~
          ("resamplingStrategy" -> "ResampleToMean") ~
          ("numBags" -> randomInt) ~
          ("bagSize" -> randomInt)

      val request = postRequest(json, url = s"/$APIVersion/octopus/${octopus.id}")

      // send the request and make sure it executes
      val response = Await.result(client(request))
      assert(response.contentType === Some(JsonHeader))
      assert(response.status === Status.Ok)
      assert(response.contentString.nonEmpty)

      // ensure that the data is correct...
      val patchOctopus = parse(response.contentString).extract[Octopus]
      assert(patchOctopus.description === TestStr)
      assert(patchOctopus.name === TestStr)
      assert(patchOctopus.dateCreated.isBefore(patchOctopus.dateModified))
      assert(octopus.ssds === patchOctopus.ssds)
      assert(octopus.ontologies === patchOctopus.ontologies)
      assert(octopus.dateCreated.isEqual(patchOctopus.dateCreated))

      // check the schema matcher model of the updated octopus
      assert(ModelStorage.get(patchOctopus.lobsterID).isDefined)
      val patchLobster = ModelStorage.get(patchOctopus.lobsterID).get
      assert(patchLobster.dateCreated.isBefore(patchLobster.dateModified))
      assert(patchLobster.resamplingStrategy === RESAMPLE_TO_MEAN)
      assert(patchLobster.numBags === randomInt)
      assert(patchLobster.bagSize === randomInt)

    } finally {
      deleteOctopi()
      assertClose()
    }
  })

  test("POST /v1.0/octopus/id responds BadRequest due to non-existent ssds") (new TestServer {
    try {

      val octopus = createOctopus()
      val PauseTime = 1000

      Thread.sleep(PauseTime)

      val TestStr = randomString
      val randomInt = genID
      val dummySeqInt = List(1, 2, 3)

      val json =
        ("description" -> TestStr) ~
          ("name" -> TestStr) ~
          ("resamplingStrategy" -> "ResampleToMean") ~
          ("ssds" -> dummySeqInt) ~
          ("numBags" -> randomInt) ~
          ("bagSize" -> randomInt)

      val request = postRequest(json, url = s"/$APIVersion/octopus/${octopus.id}")

      // send the request and make sure it executes
      val response = Await.result(client(request))
      assert(response.contentType === Some(JsonHeader))
      assert(response.status === Status.BadRequest)
      assert(response.contentString.nonEmpty)

      // ensure that no update happened
      val patchOctopus = OctopusStorage.get(octopus.id).get
      assert(patchOctopus.description === octopus.description)
      assert(patchOctopus.dateCreated.isEqual(patchOctopus.dateModified))
      assert(octopus.ssds === patchOctopus.ssds)
      assert(octopus.ontologies === patchOctopus.ontologies)

      // check the schema matcher model
      assert(ModelStorage.get(patchOctopus.lobsterID).isDefined)
      val patchLobster = ModelStorage.get(patchOctopus.lobsterID).get

      assert(patchLobster.dateCreated.isEqual(patchLobster.dateModified))
      assert(octopus.dateCreated.isEqual(patchOctopus.dateCreated))

    } finally {
      deleteOctopi()
      assertClose()
    }
  })

  test("POST /v1.0/octopus/id responds BadRequest due to non-existent owls") (new TestServer {
    try {

      val octopus = createOctopus()
      val PauseTime = 1000

      Thread.sleep(PauseTime)

      val TestStr = randomString
      val randomInt = genID
      val dummySeqInt = List(1, 2, 3)

      val json =
        ("description" -> TestStr) ~
          ("name" -> TestStr) ~
          ("resamplingStrategy" -> "ResampleToMean") ~
          ("ontologies" -> dummySeqInt) ~
          ("numBags" -> randomInt) ~
          ("bagSize" -> randomInt)

      val request = postRequest(json, url = s"/$APIVersion/octopus/${octopus.id}")

      // send the request and make sure it executes
      val response = Await.result(client(request))
      assert(response.contentType === Some(JsonHeader))
      assert(response.status === Status.BadRequest)
      assert(!response.contentString.isEmpty)

      // ensure that no update happened
      val patchOctopus = OctopusStorage.get(octopus.id).get
      assert(patchOctopus.description === octopus.description)
      assert(patchOctopus.dateCreated.isEqual(patchOctopus.dateModified))
      assert(octopus.ssds === patchOctopus.ssds)
      assert(octopus.ontologies === patchOctopus.ontologies)

      // check the schema matcher model
      assert(ModelStorage.get(patchOctopus.lobsterID).isDefined)
      val patchLobster = ModelStorage.get(patchOctopus.lobsterID).get
      assert(patchLobster.dateCreated.isEqual(patchLobster.dateModified))
      assert(octopus.dateCreated.isEqual(patchOctopus.dateCreated))

    } finally {
      deleteOctopi()
      assertClose()
    }
  })

  //==============================================================================
  // Tests for octopus training endpoint

  test("POST /v1.0/octopus/1/train returns octopus not found") (new TestServer {
    try {
      // sending training request
      val trainResp = postRequest(json = JObject(), url = s"/$APIVersion/octopus/1/train")

      val response = Await.result(client(trainResp))

      println(response.contentString)
      assert(response.status === Status.NotFound)
      assert(response.contentString.nonEmpty)

    } finally {
      assertClose()
    }
  })

  test("POST /v1.0/octopus/:id/train accepts it and completes")(new TestServer {
    try {
      val PollTime = 2000
      val PollIterations = 20

      val octopus = trainOctopus()

      val trained = pollOctopusState(octopus, PollIterations, PollTime)
      val state = concurrent.Await.result(trained, 30 seconds)
      assert(state === Training.Status.COMPLETE)

      // get the model state
      assert(ModelStorage.get(octopus.lobsterID).nonEmpty)
      val model = ModelStorage.get(octopus.lobsterID).get
      assert(model.state.status === Training.Status.COMPLETE)


    } finally {
      deleteOctopi()
      assertClose()
    }
  })

  test("POST /v1.0/octopus/:id/alignment returns alignment for a trained octopus")(new TestServer {
    try {
      val PollTime = 2000
      val PollIterations = 20

      val octopus = trainOctopus()

      val trained = pollOctopusState(octopus, PollIterations, PollTime)
      val state = concurrent.Await.result(trained, 30 seconds)
      assert(state === Training.Status.COMPLETE)

      // get the model state
      assert(ModelStorage.get(octopus.lobsterID).nonEmpty)
      val model = ModelStorage.get(octopus.lobsterID).get
      assert(model.state.status === Training.Status.COMPLETE)

      // get alignment
      val response = get(s"/$APIVersion/octopus/${octopus.id}/alignment")
      val json: JValue = parse(response.contentString)
      println(json)
      println(json \ "nodes")
      println(json \ "links")
      val jList = json.extract[String]
      println(jList)
      assert(response.contentType === Some(JsonHeader))
      assert(response.status === Status.Ok)
      assert(response.contentString.nonEmpty)


    } finally {
      deleteOctopi()
      assertClose()
    }
  })

  test("POST /v1.0/octopus/:id/train does not execute training for a trained model")(new TestServer {
    try {
      val PollTime = 2000
      val PollIterations = 20

      val octopus = trainOctopus()

      val trained = pollOctopusState(octopus, PollIterations, PollTime)
      val state = concurrent.Await.result(trained, 30 seconds)
      assert(state === Training.Status.COMPLETE)
      val dateChanged = OctopusStorage.get(octopus.id).get.state.dateChanged

      val req = postRequest(json = JObject(), url = s"/$APIVersion/octopus/${octopus.id}/train")
      // send the request and make sure it executes
      val resp = Await.result(client(req))

      assert(resp.status === Status.Accepted)
      assert(resp.contentString.isEmpty)
      // states of the models should not change!
      assert(OctopusStorage.get(octopus.id).get.state.status === Training.Status.COMPLETE)
      assert(OctopusStorage.get(octopus.id).get.state.dateChanged.isEqual(dateChanged))
      assert(ModelStorage.get(octopus.lobsterID).get.state.status === Training.Status.COMPLETE)

    } finally {
      deleteOctopi()
      assertClose()
    }
  })

  test("POST /v1.0/octopus/:id/train does not execute training for a busy model")(new TestServer {
    try {

      val smallTime = 10 // this is kind of random
      val octopus = trainOctopus()

      // wait a bit for the state to change
      Thread.sleep(smallTime)
      assert(OctopusStorage.get(octopus.id).get.state.status === Training.Status.BUSY)

      val req = postRequest(json = JObject(), url = s"/$APIVersion/octopus/${octopus.id}/train")
      // send the request and make sure it executes
      val resp = Await.result(client(req))

      assert(resp.status === Status.Accepted)
      assert(resp.contentString.isEmpty)


    } finally {
      deleteOctopi()
      assertClose()
    }
  })

  test("POST /v1.0/octopus/:id/train needs to be re-run on an updated octopus")(new TestServer {
    try {
      val PollTime = 2000
      val PollIterations = 20

      val octopus = trainOctopus()

      val trained = pollOctopusState(octopus, PollIterations, PollTime)
      val state = concurrent.Await.result(trained, PollIterations * PollTime * 2 seconds)
      assert(state === Training.Status.COMPLETE)

      // create an update json for the octopus
      val TestStr = randomString
      val randomInt = genID
      val json =
        ("description" -> TestStr) ~
          ("name" -> TestStr) ~
          ("resamplingStrategy" -> "ResampleToMean") ~
          ("numBags" -> randomInt) ~
          ("bagSize" -> randomInt)

      val request = postRequest(json, url = s"/$APIVersion/octopus/${octopus.id}")

      // send the request and make sure it executes
      val response = Await.result(client(request))
      assert(response.status === Status.Ok)

      // ensure that the data is correct and the state is back to UNTRAINED
      val patchOctopus = parse(response.contentString).extract[Octopus]
      assert(patchOctopus.state.status === Training.Status.UNTRAINED)

    } finally {
      deleteOctopi()
      assertClose()
    }
  })

  test("POST /v1.0/octopus/:id/train needs to be re-run in case model was updated")(new TestServer {
    try {
      val PollTime = 2000
      val PollIterations = 20

      val octopus = trainOctopus()

      val trained = pollOctopusState(octopus, PollIterations, PollTime)
      val state = concurrent.Await.result(trained, PollIterations * PollTime * 2 seconds)
      assert(state === Training.Status.COMPLETE)

      // create an update json for the model
      val TestStr = randomString
      val randomInt = genID
      val json =
        ("description" -> TestStr) ~
          ("resamplingStrategy" -> "ResampleToMean") ~
          ("numBags" -> randomInt) ~
          ("bagSize" -> randomInt)

      val request = postRequest(json, url = s"/$APIVersion/model/${octopus.lobsterID}")

      // send the request and make sure it executes
      val response = Await.result(client(request))
      assert(response.status === Status.Ok)

      // ensure that the data is correct and the state is back to UNTRAINED
      val patchModel = parse(response.contentString).extract[Model]
      assert(patchModel.state.status === Training.Status.UNTRAINED)

      assert(!OctopusInterface.checkTraining(octopus.id))

    } finally {
      deleteOctopi()
      assertClose()
    }
  })

  //==============================================================================
  // Tests for octopus prediction endpoint

  test("POST /v1.0/octopus/1/predict/1 returns octopus NotFound")(new TestServer {
    try {
      // now make a prediction with basically nothing
      val request = postRequest(json = JObject(),
        url = s"/$APIVersion/octopus/1/predict/${datasetMap("getCities")}")

      val response = Await.result(client(request))

      assert(!response.contentString.isEmpty)
      assert(response.status === Status.NotFound)


    } finally {
      deleteOctopi()
      assertClose()
    }
  })

  test("POST /v1.0/octopus/:id/predict/:id fails since octopus is not trained")(new TestServer {
    try {
      val octopus = createOctopus()

      // now make a prediction
      val request = postRequest(json = JObject(),
        url = s"/$APIVersion/octopus/${octopus.id}/predict/${datasetMap("getCities")}")

      val response = Await.result(client(request))

      assert(response.contentString.nonEmpty)
      assert(response.status === Status.BadRequest)


    } finally {
      deleteOctopi()
      assertClose()
    }
  })

  test("POST /v1.0/octopus/:id/predict/1 returns dataset NotFound")(new TestServer {
    try {
      val PollTime = 2000
      val PollIterations = 20

      // create a default octopus and train it
      val octopus = trainOctopus()

      val trained = pollOctopusState(octopus, PollIterations, PollTime)
      val state = concurrent.Await.result(trained, PollIterations * PollTime * 2 seconds)
      assert(state === Training.Status.COMPLETE)

      // now make a prediction
      val request = postRequest(json = JObject(),
        url = s"/$APIVersion/octopus/${octopus.id}/predict/1")

      val response = Await.result(client(request))

      assert(response.contentString.nonEmpty)
      assert(response.status === Status.NotFound)


    } finally {
      deleteOctopi()
      assertClose()
    }
  })

  test("POST /v1.0/octopus/:id/predict/:id succeeds")(new TestServer {
    try {
      val PollTime = 3000
      val PollIterations = 20
      val dummyID = 1000

      // create a default octopus and train it
      val octopus = trainOctopus()

      val trained = pollOctopusState(octopus, PollIterations, PollTime)
      val state = concurrent.Await.result(trained, PollIterations * PollTime * 2 seconds)
      assert(state === Training.Status.COMPLETE)

      assert(DatasetStorage.get(datasetMap("getCities")).isDefined)
      // now make a prediction
      val request = postRequest(json = JObject(),
        url = s"/$APIVersion/octopus/${octopus.id}/predict/${datasetMap("getCities")}")

      val response = Await.result(client(request))

      assert(response.status === Status.Ok)

      val ssdPred = parse(response.contentString).extract[SsdResults]

      assert(ssdPred.predictions.size === 10)
      val predictedSSDs: List[Ssd] = ssdPred.predictions.map(_.ssd.toSsd(dummyID).get)
      // Karma should return consistent and complete semantic models
      assert(predictedSSDs.forall(_.isComplete))
      assert(predictedSSDs.forall(_.isConsistent))
      assert(predictedSSDs.forall(_.mappings.isDefined))
      assert(predictedSSDs.forall(_.mappings.forall(_.mappings.size == 2)))

//      ssdPred.predictions.foreach(x => println(x._2))

      assert(ssdPred.predictions.forall(_.score.nodeCoherence == 1))
      assert(ssdPred.predictions.forall(_.score.nodeCoverage == 1))

      val scores = ssdPred.predictions.head.score
      assert(scores.linkCost === 5)


    } finally {
      deleteOctopi()
      assertClose()
    }
  })

  // TODO: test for a malformed csv dataset
  test("POST /v1.0/octopus/:id/predict/:id fails for a malformed dataset")(new TestServer {
    try {
      val PollTime = 3000
      val PollIterations = 20
      val dummyID = 1000

      // create a default octopus and train it
      val octopus = trainOctopus()

      val trained = pollOctopusState(octopus, PollIterations, PollTime)
      val state = concurrent.Await.result(trained, PollIterations * PollTime * 2 seconds)
      assert(state === Training.Status.COMPLETE)

      val document = new File(getClass.getResource("/malformed.csv").toURI)
      val ds = createDataset(document) match {
        case Success(d) => d
        case Failure(err) =>
          fail(err.getMessage)
      }
      val request = postRequest(json = JObject(),
        url = s"/$APIVersion/octopus/${octopus.id}/predict/${ds.id}")

      val response = Await.result(client(request))

      println(response.contentString)
      assert(response.status === Status.InternalServerError)


    } finally {
      deleteOctopi()
      assertClose()
    }
  })

}
