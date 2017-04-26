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
import java.nio.file.{Files, Paths}

import au.csiro.data61.core.api.{BadRequestException, OctopusRequest, SsdResults}
import au.csiro.data61.core.storage.{JsonFormats, OctopusStorage, SsdStorage}
import au.csiro.data61.core.drivers.OctopusInterface
import au.csiro.data61.core.storage._
import au.csiro.data61.types.ModelType.RANDOM_FOREST
import au.csiro.data61.types.ModelTypes.Model
import au.csiro.data61.types.SsdTypes.{Octopus, Owl, OwlDocumentFormat}
import au.csiro.data61.types.SamplingStrategy.NO_RESAMPLING
import au.csiro.data61.types.Training.Status
import au.csiro.data61.types._
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FileUtils
import org.joda.time.DateTime
import org.json4s.jackson.JsonMethods._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import scala.annotation.tailrec
import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}



/**
  * Tests for the OctopusInterface methods
  */
@RunWith(classOf[JUnitRunner])
class OctopusSpec extends FunSuite with JsonFormats with BeforeAndAfterEach with LazyLogging{

  override def afterEach(): Unit = {
    // the order of removal now matters!
    OctopusStorage.removeAll()
    ModelStorage.removeAll()
    SsdStorage.removeAll()
    DatasetStorage.removeAll()
    OwlStorage.removeAll()
  }

  val businessSsdID = 0
  val exampleOwlID = 1
  val dummyID = 1000

  override def beforeEach(): Unit = {
    copySampleDatasets() // copy csv files for getCities and businessInfo
    SsdStorage.add(businessSSD.id, businessSSD) // add businessInfo ssd
    OwlStorage.add(exampleOwl.id, exampleOwl)  // add sample ontology
    // write owl file
    Try{
      val stream = new FileInputStream(exampleOntolPath.toFile)
      OwlStorage.writeOwlDocument(exampleOwlID, stream)
    }

  }

  val ssdDir = getClass.getResource("/ssd").getPath

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

  val owlDir = getClass.getResource("/owl").getPath
  val exampleOntolPath = Paths.get(owlDir,"dataintegration_report_ontology.ttl")
  val exampleOwl = Owl(
    id = exampleOwlID,
    name = exampleOntolPath.toString,
    format = OwlDocumentFormat.Turtle,
    description = "sample",
    dateCreated = DateTime.now,
    dateModified = DateTime.now)

  val partialSSD: Ssd = readSSD(Paths.get(ssdDir,"partial_model.ssd").toString)
  val veryPartialSSD: Ssd = readSSD(Paths.get(ssdDir,"partial_model2.ssd").toString)
  val emptyCitiesSSD: Ssd = readSSD(Paths.get(ssdDir,"empty_getCities.ssd").toString)
  val emptySSD: Ssd = readSSD(Paths.get(ssdDir,"empty_model.ssd").toString)
  val businessSSD: Ssd = readSSD(Paths.get(ssdDir,"businessInfo.ssd").toString)

  val defaultFeatures = FeaturesConfig(
    activeFeatures = Set("num-unique-vals", "prop-unique-vals", "prop-missing-vals",
    "ratio-alpha-chars", "prop-numerical-chars",
    "prop-whitespace-chars", "prop-entries-with-at-sign",
    "prop-entries-with-hyphen", "prop-entries-with-paren",
    "prop-entries-with-currency-symbol", "mean-commas-per-entry",
    "mean-forward-slashes-per-entry",
    "prop-range-format", "is-discrete", "entropy-for-discrete-values"),
    activeGroupFeatures = Set.empty[String],
    featureExtractorParams = Map()
  )

  val defaultOctopusRequest = OctopusRequest(
    name = None,
    description = Some("default octopus"),
    modelType = None,
    features = Some(defaultFeatures),
    resamplingStrategy = Some(NO_RESAMPLING),
    numBags = None,
    bagSize = None,
    ontologies = None,
    ssds = Some(List(businessSSD.id)),
    modelingProps = None)

  val someOctopusRequest = OctopusRequest(
    name = None,
    description = Some("default octopus"),
    modelType = None,
    features = Some(defaultFeatures),
    resamplingStrategy = Some(NO_RESAMPLING),
    numBags = None,
    bagSize = None,
    ontologies = None,
    ssds = Some(List(businessSSD.id)),
    modelingProps = Some(ModelingProperties(sizeWeight = 1.0)))


  val blankOctopusRequest = OctopusRequest(None, None, None, None, None, None, None, None, None, None)

  val helperDir = getClass.getResource("/helper").getPath
  val sampleDsDir = getClass.getResource("/sample.datasets").getPath
  val datasetMap = Map("businessInfo" -> 767956483, "getCities" -> 696167703)
  val businessDsPath = Paths.get(sampleDsDir,
    datasetMap("businessInfo").toString, datasetMap("businessInfo").toString + ".json")
  val citiesDsPath = Paths.get(sampleDsDir,
    datasetMap("getCities").toString, datasetMap("getCities").toString + ".json")

  // karma stuff
  val karmaDir = getClass.getResource("/karma").getPath
  val exampleKarmaSSD: String = Paths.get(karmaDir,"businessInfo.csv.model.json") toString
  val businessAlign: String = Paths.get(karmaDir,"align_business.json") toString
  val businessCitiesAlign: String = Paths.get(karmaDir,"align_business_cities.json") toString

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
      case Success(ds) =>
        ds
      case Failure(err) =>
        fail(err.getMessage)
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
    * @param octopus
    * @param pollIterations
    * @param pollTime
    * @return
    */
  def pollOctopusState(octopus: Octopus, pollIterations: Int, pollTime: Int)
  : Future[Training.Status] = {
    Future {

      def state(): Training.Status = {
        Thread.sleep(pollTime)
        // build a request to get the model...
        val m = OctopusInterface.get(octopus.id) match {
          case None => throw new Exception("Failed to retrieve model state")
          case Some(octo) => octo
        }
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

  test("Creating octopus for businessInfo") {
    // create default octopus
    val octopus = OctopusInterface.createOctopus(defaultOctopusRequest)  match {
      case Success(octo) => octo
      case _ => fail("Problems with creation of octopus!")
    }
    // lobster should automatically be created
    val lobster: Model = ModelStorage.get(octopus.lobsterID).get

    assert(octopus.ssds === List(businessSsdID))
    assert(octopus.ontologies === List(exampleOwlID))
    assert(octopus.state.status === Status.UNTRAINED)

    assert(lobster.modelType === RANDOM_FOREST)
    assert(lobster.resamplingStrategy === NO_RESAMPLING)
    assert(lobster.classes.size === 5)
    assert(lobster.labelData.size === 4)
    assert(lobster.labelData === Map(
      643243447 -> "Organization---name",
      1534291035 -> "Person---name",
      843054462 -> "City---name",
      1138681944 -> "State---name")
    )
  }

  test("Training octopus with businessInfo succeeds") {
    // create default octopus
    val octopus = OctopusInterface.createOctopus(defaultOctopusRequest) match {
      case Success(octo) => octo
      case _ => fail("Problems with creation of octopus!")
    }
    // get it from the storage
    val storedOctopus = OctopusStorage.get(octopus.id).get

    assert(octopus.id === storedOctopus.id)
    assert(octopus.lobsterID === storedOctopus.lobsterID)
    assert(octopus.state === storedOctopus.state)
    assert(octopus.ssds.sorted === storedOctopus.ssds.sorted)
    assert(octopus.ontologies.sorted === storedOctopus.ontologies.sorted)
    assert(octopus.semanticTypeMap === storedOctopus.semanticTypeMap)

    val state = OctopusInterface.trainOctopus(octopus.id)

    val PollTime = 2000
    val PollIterations = 20
    val smallTime = 10
    // octopus becomes busy
    assert(state.get.status === Status.BUSY)
    Thread.sleep(smallTime)
    // lobster becomes busy
    assert(ModelStorage.get(octopus.lobsterID).get.state.status === Status.BUSY)

    // wait for the trainig to complete
    val trained = pollOctopusState(octopus, PollIterations, PollTime)
    val octopusState = concurrent.Await.result(trained, 30 seconds)

    assert(ModelStorage.get(octopus.lobsterID).get.state.status === Status.COMPLETE)
    assert(octopusState === Status.COMPLETE)

    // (if the training succeeds) alignment graph json is located at:
    val alignmentGraphJson: String = OctopusStorage.getAlignmentGraphPath(octopus.id).toString
    assert(Files.exists(Paths.get(alignmentGraphJson)))

    // kind-of reading in the alignment graph as our semantic model
    val alignSM: SemanticModel = KarmaTypes.readAlignmentGraph(
      OctopusStorage.getAlignmentGraphPath(octopus.id).toString)

    assert(alignSM.isConnected)  // the alignment graph must be connected

    // this is what we approx should get
    val karmaAlign: SemanticModel = KarmaTypes.readAlignmentGraph(businessAlign)

    assert(alignSM.getLinks.size === karmaAlign.getLinks.size)
    assert(alignSM.getLinkLabels === karmaAlign.getLinkLabels)
    assert(alignSM.getNodeLabels === karmaAlign.getNodeLabels)
    assert(alignSM.getNodes.size === karmaAlign.getNodes.size)
  }

  test("Predicting with octopus for cities fails since octopus is not trained") {
    // create default octopus
    val octopus = OctopusInterface.createOctopus(defaultOctopusRequest) match {
      case Success(octo) => octo
      case _ => fail("Problems with creation of octopus!")
    }
    // get it from the storage
    val storedOctopus = OctopusStorage.get(octopus.id).get

    // octopus is untrained
    assert(octopus.state.status === Status.UNTRAINED)

    Try {
      OctopusInterface.predictOctopus(octopus.id, datasetMap("getCities"))
    } match {
      case Success(_) =>
        fail("Octopus is not trained. Prediction should fail!")
      case Failure(err: BadRequestException) =>
        succeed
      case Failure(err) =>
        fail(s"Wrong failure: $err")
    }
  }

  test("Predicting with octopus for cities succeeds") {
    // create default octopus
    val octopus: Octopus = OctopusInterface.createOctopus(someOctopusRequest) match {
      case Success(octo) => octo
      case _ => fail("Problems with creation of octopus!")
    }
    val state = OctopusInterface.trainOctopus(octopus.id)


    val PollTime = 2000
    val PollIterations = 20
    // wait for the trainig to complete
    val trained = pollOctopusState(octopus, PollIterations, PollTime)
    val octopusState = concurrent.Await.result(trained, PollTime * PollIterations * 2 seconds)

    assert(ModelStorage.get(octopus.lobsterID).get.state.status === Status.COMPLETE)
    assert(OctopusStorage.get(octopus.id).get.state.status === Status.COMPLETE)

    val recommends = Try {
      OctopusInterface.predictOctopus(octopus.id, datasetMap("getCities"))
    } toOption

    recommends match {
      case Some(ssdPred: SsdResults) =>
        assert(ssdPred.predictions.size === 10)
        val predictedSSDs: List[Ssd] = ssdPred.predictions.map(_.ssd.toSsd(dummyID).get)
        // Karma should return consistent and complete semantic models
        assert(predictedSSDs.forall(_.isComplete))
        assert(predictedSSDs.forall(_.isConsistent))
        assert(predictedSSDs.forall(_.mappings.isDefined))
        assert(predictedSSDs.forall(_.mappings.forall(_.mappings.size == 2)))

        assert(ssdPred.predictions.forall(_.score.nodeCoherence == 1))
        assert(ssdPred.predictions.forall(_.score.nodeCoverage == 1))

        val scores = ssdPred.predictions.head.score
        assert(scores.linkCost === 5)

      case _ =>
        fail("Problems here since there are no recommendations :(")
    }
  }

  // tests for createOctopus
  // tests for trainOctopus
  // tests for predictOctopus


//  test("Recommendation for empty businessInfo.csv succeeds"){
//    SSDStorage.add(businessSSD.id, businessSSD)
//    // first, we build the Alignment Graph = training step
//    val karmaTrain = KarmaBuildAlignmentGraph(karmaWrapper)
//    // our alignment
//    var alignment = karmaTrain.alignment
//    assert(alignment.getGraph.vertexSet.size === 0)
//    assert(alignment.getGraph.edgeSet.size === 0)
//
//    alignment = karmaTrain.constructInitialAlignment(knownSSDs)
//    assert(alignment.getGraph.vertexSet.size === 8)
//    assert(alignment.getGraph.edgeSet.size === 7)
//
//    val newSSD = Try {
//      val stream = new FileInputStream(Paths.get(emptySSD).toFile)
//      parse(stream).extract[SemanticSourceDesc]
//    } match {
//      case Success(ssd) =>
//        ssd
//      case Failure(err) =>
//        fail(err.getMessage)
//    }
//    // check uploaded ontologies....
//    assert(karmaWrapper.ontologies.size === 1)
//    assert(karmaWrapper.karmaWorkspace.getOntologyManager.getClasses.size === 7)
//    assert(karmaWrapper.karmaWorkspace.getOntologyManager.getDataProperties.size === 9)
//    assert(karmaWrapper.karmaWorkspace.getOntologyManager.getObjectProperties.size === 12)
//
//    // now, we run prediction for the new SSD
//    karmaWrapper = KarmaParams(alignmentDir, List(exampleOntol), None)
//    val karmaPredict = KarmaSuggestModel(karmaWrapper)
//    val recommends = karmaPredict
//      .suggestModels(newSSD, List(exampleOntol), getBusinessDataSetPredictions, businessSemanticTypeMap, businessAttrToColMap2)
//
//    recommends match {
//      case Some(ssdPred: SSDPrediction) =>
//        assert(ssdPred.suggestions.size === 1)
//        val recSemanticModel = ssdPred.suggestions(0)._1
//        val scores = ssdPred.suggestions(0)._2
//
//        assert(recSemanticModel.isConsistent) // Karma should return a consistent and complete semantic model
//        assert(recSemanticModel.isComplete)
//        assert(scores.nodeCoherence === 1)
//        assert(scores.nodeConfidence === 1) // confidence is one since Karma standardizes scores so that sum=1
//        assert(scores.linkCost === 7)
//        assert(scores.nodeCoverage === 1)
//        assert(recSemanticModel.mappings.isDefined)
//        assert(recSemanticModel.mappings.forall(_.mappings.size==4))
//      case _ =>
//        fail("Wrong! There should be some prediction!")
//    }
//  }
//
//  test("Recommendation for partial businessInfo.csv with no matcher predictions succeeds"){
//    SSDStorage.add(businessSSD.id, businessSSD)
//    logger.info("================================================================")
//    val karmaTrain = KarmaBuildAlignmentGraph(karmaWrapper)
//    // our alignment
//    logger.info("================================================================")
//    var alignment = karmaTrain.alignment
//    assert(alignment.getGraph.vertexSet.size === 0)
//    assert(alignment.getGraph.edgeSet.size === 0)
//
//    logger.info("================================================================")
//    alignment = karmaTrain.constructInitialAlignment(knownSSDs)
//    assert(alignment.getGraph.vertexSet.size === 8)
//    assert(alignment.getGraph.edgeSet.size === 7)
//
//    logger.info("================================================================")
//    val newSSD = Try {
//      val stream = new FileInputStream(Paths.get(partialSSD).toFile)
//      parse(stream).extract[SemanticSourceDesc]
//    } match {
//      case Success(ssd) =>
//        ssd
//      case Failure(err) =>
//        fail(err.getMessage)
//    }
//    // check uploaded ontologies....
//    assert(karmaWrapper.ontologies.size === 1)
//    assert(karmaWrapper.karmaWorkspace.getOntologyManager.getClasses.size === 7)
//    assert(karmaWrapper.karmaWorkspace.getOntologyManager.getDataProperties.size === 9)
//    assert(karmaWrapper.karmaWorkspace.getOntologyManager.getObjectProperties.size === 12)
//
//    logger.info("================================================================")
//    karmaWrapper = KarmaParams(alignmentDir, List(exampleOntol), None)
//    val karmaPredict = KarmaSuggestModel(karmaWrapper)
//    logger.info("================================================================")
//    val recommends = karmaPredict
//      .suggestModels(newSSD, List(exampleOntol), None, businessSemanticTypeMap, businessAttrToColMap2)
//    recommends match {
//      case Some(ssdPred: SSDPrediction) =>
//        assert(ssdPred.suggestions.size === 1)
//        val recSemanticModel = ssdPred.suggestions(0)._1
//        val scores = ssdPred.suggestions(0)._2
//
//        val str = compact(Extraction.decompose(recSemanticModel))
//        val outputPath = Paths.get(ModelerConfig.KarmaDir, s"recommended_ssd.json")
//        // write the object to the file system
//        logger.debug("HERE")
//        Files.write(
//          outputPath,
//          str.getBytes(StandardCharsets.UTF_8)
//        )
//
//        assert(recSemanticModel.isConsistent)
//        assert(recSemanticModel.isComplete)
//        assert(scores.nodeCoherence === 1) // all column node mappings are user provided
//        assert(scores.nodeConfidence === 1)
//        assert(scores.linkCost === 7)
//        assert(scores.nodeCoverage === 1)
//        assert(recSemanticModel.mappings.isDefined)
//        assert(recSemanticModel.mappings.forall(_.mappings.size==4))
//      //        assert(recSemanticModel.mappings === Some(SSDMapping(Map(4 -> 7, 5 -> 5, 6 -> 4, 7 -> 6)))) // unfortunately, mappings are not fixed
//      case _ =>
//        fail("Wrong!")
//    }
//  }
//
//  test("Recommendation for partially specified businessInfo.csv with matcher predictions succeeds"){
//    SSDStorage.add(businessSSD.id, businessSSD)
//    logger.info("================================================================")
//    val karmaTrain = KarmaBuildAlignmentGraph(karmaWrapper)
//    // our alignment
//    logger.info("================================================================")
//    var alignment = karmaTrain.alignment
//    assert(alignment.getGraph.vertexSet.size === 0)
//    assert(alignment.getGraph.edgeSet.size === 0)
//
//    logger.info("================================================================")
//    alignment = karmaTrain.constructInitialAlignment(knownSSDs)
//    assert(alignment.getGraph.vertexSet.size === 8)
//    assert(alignment.getGraph.edgeSet.size === 7)
//
//    logger.info("================================================================")
//    val newSSD = Try {
//      val stream = new FileInputStream(Paths.get(veryPartialSSD).toFile)
//      parse(stream).extract[SemanticSourceDesc]
//    } match {
//      case Success(ssd) =>
//        ssd
//      case Failure(err) =>
//        fail(err.getMessage)
//    }
//    // check uploaded ontologies....
//    assert(karmaWrapper.ontologies.size === 1)
//    assert(karmaWrapper.karmaWorkspace.getOntologyManager.getClasses.size === 7)
//    assert(karmaWrapper.karmaWorkspace.getOntologyManager.getDataProperties.size === 9)
//    assert(karmaWrapper.karmaWorkspace.getOntologyManager.getObjectProperties.size === 12)
//
//    logger.info("================================================================")
//    karmaWrapper = KarmaParams(alignmentDir, List(exampleOntol), None)
//    val karmaPredict = KarmaSuggestModel(karmaWrapper)
//    logger.info("================================================================")
//    val recommends = karmaPredict
//      .suggestModels(newSSD, List(exampleOntol), getBusinessDataSetPredictions, businessSemanticTypeMap, businessAttrToColMap2)
//    recommends match {
//      case Some(ssdPred: SSDPrediction) =>
//        assert(ssdPred.suggestions.size === 1)
//        val recSemanticModel = ssdPred.suggestions(0)._1
//        val scores = ssdPred.suggestions(0)._2
//
//        val str = compact(Extraction.decompose(recSemanticModel))
//        val outputPath = Paths.get(ModelerConfig.KarmaDir, s"recommended_ssd.json")
//        // write the object to the file system
//        Files.write(
//          outputPath,
//          str.getBytes(StandardCharsets.UTF_8)
//        )
//
//        assert(recSemanticModel.isConsistent)
//        assert(recSemanticModel.isComplete)
//        assert(scores.nodeCoherence === 1) // all column node mappings are user provided
//        assert(scores.nodeConfidence === 1)
//        assert(scores.linkCost === 7)
//        assert(scores.nodeCoverage === 1)
//        assert(recSemanticModel.mappings.isDefined)
//        assert(recSemanticModel.mappings.forall(_.mappings.size==4))
//      //assert(recSemanticModel.mappings === Some(SSDMapping(Map(4 -> 7, 5 -> 5, 6 -> 4, 7 -> 6)))) // unfortunately, mappings are not fixed
//      case _ =>
//        fail("Wrong!")
//    }
//  }
//
//  test("Recommendation for empty getCities.csv succeeds"){
//    SSDStorage.add(businessSSD.id, businessSSD)
//    // first, we build the Alignment Graph = training step
//    val karmaTrain = KarmaBuildAlignmentGraph(karmaWrapper)
//    // our alignment
//    var alignment = karmaTrain.alignment
//    assert(alignment.getGraph.vertexSet.size === 0)
//    assert(alignment.getGraph.edgeSet.size === 0)
//
//    alignment = karmaTrain.constructInitialAlignment(knownSSDs)
//    assert(alignment.getGraph.vertexSet.size === 8)
//    assert(alignment.getGraph.edgeSet.size === 7)
//
//    val newSSD = Try {
//      val stream = new FileInputStream(Paths.get(emptyCitiesSSD).toFile)
//      parse(stream).extract[SemanticSourceDesc]
//    } match {
//      case Success(ssd) =>
//        ssd
//      case Failure(err) =>
//        fail(err.getMessage)
//    }
//    // check uploaded ontologies....
//    assert(karmaWrapper.ontologies.size === 1)
//    assert(karmaWrapper.karmaWorkspace.getOntologyManager.getClasses.size === 7)
//    assert(karmaWrapper.karmaWorkspace.getOntologyManager.getDataProperties.size === 9)
//    assert(karmaWrapper.karmaWorkspace.getOntologyManager.getObjectProperties.size === 12)
//
//    // now, we run prediction for the new SSD
//    karmaWrapper = KarmaParams(alignmentDir, List(exampleOntol), None)
//    val karmaPredict = KarmaSuggestModel(karmaWrapper)
//    val recommends = karmaPredict
//      .suggestModels(newSSD, List(exampleOntol), getCitiesDataSetPredictions, Map(), citiesAttrToColMap)
//
//    recommends match {
//      case Some(ssdPred: SSDPrediction) =>
//        assert(ssdPred.suggestions.size === 4)
//        assert(ssdPred.suggestions.forall(_._1.isComplete)) // Karma should return a consistent and complete semantic model
//        assert(ssdPred.suggestions.forall(_._1.isConsistent))
//        assert(ssdPred.suggestions.forall(_._1.mappings.isDefined))
//        assert(ssdPred.suggestions.forall(_._1.mappings.forall(_.mappings.size==2)))
//        assert(ssdPred.suggestions.forall(_._2.nodeConfidence == 0.5))
//        assert(ssdPred.suggestions.forall(_._2.nodeCoherence == 1))
//        assert(ssdPred.suggestions.forall(_._2.nodeCoverage == 1))
//
//        ssdPred.suggestions.forall(_._1.mappings.isDefined)
//
//        val recSemanticModel = ssdPred.suggestions(0)._1
//        val scores = ssdPred.suggestions(0)._2
//        assert(scores.linkCost === 4)
//
//      case _ =>
//        fail("Wrong! There should be some prediction!")
//    }
//  }

}

