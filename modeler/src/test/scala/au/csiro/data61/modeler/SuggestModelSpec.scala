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

package au.csiro.data61.modeler

import java.io.{File, FileInputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.scalatest.junit.JUnitRunner
import com.typesafe.scalalogging.LazyLogging

import language.postfixOps
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}
import edu.isi.karma.modeling.alignment.{SemanticModel => KarmaSSD}
import au.csiro.data61.types._
import au.csiro.data61.modeler.karma.{KarmaBuildAlignmentGraph, KarmaParams, KarmaSuggestModel}
import au.csiro.data61.types.ColumnTypes.ColumnID
import au.csiro.data61.types.Exceptions.ModelerException
import au.csiro.data61.types.SsdTypes._

/**
  * Tests for KarmaSuggestModel object
  */
@RunWith(classOf[JUnitRunner])
class SuggestModelSpec  extends FunSuite with ModelerJsonFormats with BeforeAndAfterEach with LazyLogging {

  val dummyOctopusID = 1

  val ssdDir = getClass.getResource("/ssd").getPath
  val exampleSSD: String = Paths.get(ssdDir,"businessInfo.ssd") toString
  val emptySSD: String = Paths.get(ssdDir,"empty_model_2.ssd") toString
  val businessEmptySSD: String = Paths.get(ssdDir,"empty_business.ssd") toString
  val partialSSD: String = Paths.get(ssdDir,"partial_model.ssd") toString
  val veryPartialSSD: String = Paths.get(ssdDir,"partial_model2.ssd") toString
  val emptyCitiesSSD: String = Paths.get(ssdDir,"empty_getCities.ssd") toString
  val citiesSSD: String = Paths.get(ssdDir,"getCities.ssd") toString
  val personSSD: String = Paths.get(ssdDir,"personalInfo.ssd") toString

  val alignmentDir = Paths.get("/tmp/test-ssd", "alignment") toString
  val exampleOntol: String = Paths.get(ssdDir,"dataintegration_report_ontology.ttl") toString

  var knownSSDs: List[Ssd] = List() // has the function of SSDStorage
  var karmaWrapper = KarmaParams(alignmentDir, List(), None)

  val unknownThreshold = 0.49

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

  def addSSD(ssdPath: String): Unit = {

    knownSSDs = readSSD(ssdPath) :: knownSSDs

    karmaWrapper.deleteKarma()
    // we need to clean the alignmentDir
    removeAll(Paths.get(alignmentDir))
    karmaWrapper = KarmaParams(alignmentDir, List(exampleOntol), None)
  }

  def removeAll(path: Path): Unit = {
    def getRecursively(f: Path): Seq[Path] =
      f.toFile.listFiles
        .filter(_.isDirectory)
        .flatMap { x => getRecursively(x.toPath) } ++
        f.toFile.listFiles.map(_.toPath)
    getRecursively(path).foreach { f =>
      if (!f.toFile.delete) {throw ModelerException(s"Failed to delete ${f.toString}")}
    }
  }

  override def afterEach(): Unit = {
    knownSSDs = List()
//    karmaWrapper.deleteKarma()
    // we need to clean the alignmentDir
//    removeAll(Paths.get(alignmentDir))
  }

  /**
    * Construct DataSetPrediction instance for businessInfo.csv
    */
  def getBusinessDataSetPredictions: Option[DataSetPrediction] = {
    // we care only about scores in the semantic-modeller
    val col0 = ColumnPrediction(label="Organization---name",
      confidence=0.5,
      scores=Map("Organization---name" -> 0.5),
      features=Map())
    val col1 = ColumnPrediction(label="Person---name",
      confidence=1.0,
      scores=Map("Person---name" -> 1.0),
      features=Map())
    val col2 = ColumnPrediction(label="City---name",
      confidence=1.0,
      scores=Map("City---name" -> 1.0),
      features=Map())
    val col3 = ColumnPrediction(label="State---name",
      confidence=1.0,
      scores=Map("State---name" -> 1.0),
      features=Map())

    Some(DataSetPrediction(
      modelID = 0,
      dataSetID = 0,
      predictions = Map("4" -> col0, "5" -> col1, "6" -> col2, "7" -> col3)
    ))
  }
  val businessSemanticTypeMap: Map[String, String] = Map()
  val businessAttrToColMap: Map[AttrID,ColumnID] = Map(0 -> 0, 1 -> 1, 2 -> 2, 3 -> 3)
  val businessAttrToColMap2: Map[AttrID,ColumnID] = Map(4 -> 4, 5 -> 5, 6 -> 6, 7 -> 7)

  /**
    * Construct DataSetPrediction instance for getCities.csv
    */
  def getCitiesDataSetPredictions: Option[DataSetPrediction] = {
    // we care only about scores in the semantic-modeller
    val col0 = ColumnPrediction(label="City---name",
      confidence=0.5,
      scores=Map("City---name" -> 0.5, "State---name" -> 0.5),
      features=Map())
    val col1 = ColumnPrediction(label="City---name",
      confidence=0.5,
      scores=Map("City---name" -> 0.5, "State---name" -> 0.5),
      features=Map())

    Some(DataSetPrediction(
      modelID = 0,
      dataSetID = 0,
      predictions = Map("10" -> col0, "11" -> col1)
    ))
  }

  /**
    * Construct DataSetPrediction instance for getCities.csv
    */
  def getCitiesDataSetPredictions2: Option[DataSetPrediction] = {
    // we care only about scores in the semantic-modeller
    val col0 = ColumnPrediction(label="City---name",
      confidence=0.6,
      scores=Map("City---name" -> 0.6, "State---name" -> 0.4),
      features=Map())
    val col1 = ColumnPrediction(label="State---name",
      confidence=0.6,
      scores=Map("City---name" -> 0.4, "State---name" -> 0.6),
      features=Map())

    Some(DataSetPrediction(
      modelID = 0,
      dataSetID = 0,
      predictions = Map("10" -> col1, "11" -> col0)
    ))
  }

  /**
    * Construct DataSetPrediction instance for getCities.csv
    */
  def getProblematicCitiesDataSetPredictions: Option[DataSetPrediction] = {
    // we care only about scores in the semantic-modeller
    val col0 = ColumnPrediction(label="City---name",
      confidence=0.5,
      scores=Map("City---name" -> 0.5, "State---name" -> 0.5),
      features=Map())
    val col1 = ColumnPrediction(label="City---name",
      confidence=0.0,
      scores=Map("City---name" -> 0.0, "State---name" -> 0.0),
      features=Map())

    Some(DataSetPrediction(
      modelID = 0,
      dataSetID = 0,
      predictions = Map("10" -> col0, "11" -> col1)
    ))
  }

  /**
    * Construct DataSetPrediction instance for getCities.csv
    */
  def getUnknownCitiesDataSetPredictions: Option[DataSetPrediction] = {
    // we care only about scores in the semantic-modeller
    val col0 = ColumnPrediction(label="City---name",
      confidence=0.5,
      scores=Map("City---name" -> 0.5, "State---name" -> 0.5),
      features=Map())
    val col1 = ColumnPrediction(label="City---name",
      confidence=1.0,
      scores=Map(ModelTypes.UknownClass -> 0.1, "City---name" -> 0.4, "State---name" -> 0.5),
      features=Map())

    Some(DataSetPrediction(
      modelID = 0,
      dataSetID = 0,
      predictions = Map("10" -> col0, "11" -> col1)
    ))
  }

  def getDiscardCitiesDataSetPredictions: Option[DataSetPrediction] = {
    // we care only about scores in the semantic-modeller
    val col0 = ColumnPrediction(label="City---name",
      confidence=0.5,
      scores=Map("City---name" -> 0.5, "State---name" -> 0.5),
      features=Map())
    val col1 = ColumnPrediction(label=ModelTypes.UknownClass,
      confidence=1.0,
      scores=Map(ModelTypes.UknownClass -> 1.0, "City---name" -> 0.0, "State---name" -> 0.0),
      features=Map())

    Some(DataSetPrediction(
      modelID = 0,
      dataSetID = 0,
      predictions = Map("10" -> col0, "11" -> col1)
    ))
  }

  def getUnknownMaxCitiesDataSetPredictions: Option[DataSetPrediction] = {
    // we care only about scores in the semantic-modeller
    val col0 = ColumnPrediction(label="City---name",
      confidence=0.5,
      scores=Map("City---name" -> 0.5, "State---name" -> 0.5),
      features=Map())
    val col1 = ColumnPrediction(label=ModelTypes.UknownClass,
      confidence=0.4,
      scores=Map(ModelTypes.UknownClass -> 0.4, "City---name" -> 0.3, "State---name" -> 0.3),
      features=Map())

    Some(DataSetPrediction(
      modelID = 0,
      dataSetID = 0,
      predictions = Map("10" -> col0, "11" -> col1)
    ))
  }

  def writeToFile(dir: Path, ssd: Ssd): Unit = {
    val str = compact(Extraction.decompose(ssd))

    // ensure that the directories exist...
    if (!dir.toFile.exists) dir.toFile.mkdirs

    val outputPath = Paths.get(dir.toString, ssd.name + ".ssd")
    // write the object to the file system
    println(s"Writing to $outputPath")
    Files.write(
      outputPath,
      str.getBytes(StandardCharsets.UTF_8)
    )
  }

  // wrong semantic type map
  val citiesSemanticTypeMap: Map[String, String] = Map(
    "City" -> "abc:City"
    , "name" -> "abc:name"
    , "State" -> "abc:State"
  )
  // correct semantic type map
  val correctCitiesSemanticTypeMap: Map[String, String] = Map(
    "City" -> TypeConfig.DefaultNamespace
    , "name" -> TypeConfig.DefaultNamespace
    , "State" -> TypeConfig.DefaultNamespace
  )

  val citiesAttrToColMap: Map[AttrID,ColumnID] = Map(10 -> 10, 11 -> 11)

  def constructKarmaSuggestModel(ssdPath: String): (Ssd, KarmaSuggestModel) = {
    addSSD(exampleSSD)
    logger.info("================================================================")
    val karmaTrain = KarmaBuildAlignmentGraph(karmaWrapper)
    // our alignment
    logger.info("================================================================")
    var alignment = karmaTrain.alignment
    assert(alignment.getGraph.vertexSet.size === 0)
    assert(alignment.getGraph.edgeSet.size === 0)

    logger.info("================================================================")
    alignment = karmaTrain.constructInitialAlignment(knownSSDs)
    assert(alignment.getGraph.vertexSet.size === 8)
    assert(alignment.getGraph.edgeSet.size === 7)

    logger.info("================================================================")
    val newSSD = readSSD(ssdPath)
    // check uploaded ontologies....
    assert(karmaWrapper.ontologies.size === 1)
    assert(karmaWrapper.karmaWorkspace.getOntologyManager.getClasses.size === 7)
    assert(karmaWrapper.karmaWorkspace.getOntologyManager.getDataProperties.size === 9)
    assert(karmaWrapper.karmaWorkspace.getOntologyManager.getObjectProperties.size === 12)

    logger.info("================================================================")
    karmaWrapper = KarmaParams(alignmentDir, List(exampleOntol), None)
    val karmaPredict = KarmaSuggestModel(karmaWrapper)
    logger.info("================================================================")
    (newSSD, karmaPredict)
  }

  //=========================Tests==============================================
  test("Recommendation for businessInfo.csv fails since there are no preloaded ontologies"){
    val newSSD = readSSD(emptySSD)

    // check uploaded ontologies....
    assert(karmaWrapper.ontologies.size === 0)
    assert(karmaWrapper.karmaWorkspace.getOntologyManager.getClasses.size != 7)
    assert(karmaWrapper.karmaWorkspace.getOntologyManager.getDataProperties.size != 9)
    assert(karmaWrapper.karmaWorkspace.getOntologyManager.getObjectProperties.size != 12)

    val karmaPredict = KarmaSuggestModel(karmaWrapper)
    val recommends = karmaPredict
      .suggestModels(
        newSSD, List(exampleOntol), getBusinessDataSetPredictions, businessSemanticTypeMap, businessAttrToColMap)
    assert(recommends.isEmpty)
  }

  test("Recommendation for businessInfo.csv fails since the alignment graph is not constructed"){
    addSSD(exampleSSD)
    val newSSD = readSSD(emptySSD)
    // check uploaded ontologies....
    assert(karmaWrapper.ontologies.size === 1)
    assert(karmaWrapper.karmaWorkspace.getOntologyManager.getClasses.size === 7)
    assert(karmaWrapper.karmaWorkspace.getOntologyManager.getDataProperties.size === 9)
    assert(karmaWrapper.karmaWorkspace.getOntologyManager.getObjectProperties.size === 12)

    val karmaPredict = KarmaSuggestModel(karmaWrapper)
    val recommends = karmaPredict
      .suggestModels(
        newSSD, List(exampleOntol), getBusinessDataSetPredictions, businessSemanticTypeMap, businessAttrToColMap)
    assert(recommends.isEmpty)
  }

  test("Recommendation for empty businessInfo.csv succeeds"){
    val (newSSD, karmaPredict) = constructKarmaSuggestModel(emptySSD)
    val recommends = karmaPredict
      .suggestModels(
        newSSD, List(exampleOntol), getBusinessDataSetPredictions, businessSemanticTypeMap, businessAttrToColMap2)

    recommends match {
      case Some(ssdPred: SsdPrediction) =>
        assert(ssdPred.suggestions.size === 1)
        val recSemanticModel = ssdPred.suggestions.head._1
        val scores = ssdPred.suggestions.head._2

        assert(recSemanticModel.isConsistent) // Karma should return a consistent and complete semantic model
        assert(recSemanticModel.isComplete)
        assert(scores.nodeCoherence === 1)
        assert(scores.nodeConfidence === 1) // confidence is one since Karma standardizes scores so that sum=1
        assert(scores.linkCost === 7)
        assert(scores.nodeCoverage === 1)
        assert(recSemanticModel.mappings.isDefined)
        assert(recSemanticModel.mappings.forall(_.mappings.size==4))
      case _ =>
        fail("Wrong! There should be some prediction!")
    }
  }

  test("Recommendation for partial businessInfo.csv with no matcher predictions succeeds"){
    val (newSSD, karmaPredict) = constructKarmaSuggestModel(partialSSD)
    val recommends = karmaPredict
      .suggestModels(newSSD, List(exampleOntol), None, businessSemanticTypeMap, businessAttrToColMap2)
    recommends match {
      case Some(ssdPred: SsdPrediction) =>
        assert(ssdPred.suggestions.size === 1)
        val recSemanticModel = ssdPred.suggestions.head._1
        val scores = ssdPred.suggestions.head._2

        val str = compact(Extraction.decompose(recSemanticModel))
        val outputPath = Paths.get(ModelerConfig.KarmaDir, s"recommended_ssd.json")
        // write the object to the file system
        logger.debug("HERE")
        Files.write(
          outputPath,
          str.getBytes(StandardCharsets.UTF_8)
        )

        assert(recSemanticModel.isConsistent)
        assert(recSemanticModel.isComplete)
        assert(scores.nodeCoherence === 1) // all column node mappings are user provided
        assert(scores.nodeConfidence === 1)
        assert(scores.linkCost === 7)
        assert(scores.nodeCoverage === 1)
        assert(recSemanticModel.mappings.isDefined)
        assert(recSemanticModel.mappings.forall(_.mappings.size==4))
//        assert(recSemanticModel.mappings === Some(SSDMapping(Map(4 -> 7, 5 -> 5, 6 -> 4, 7 -> 6)))) // unfortunately, mappings are not fixed
      case _ =>
        fail("Wrong!")
    }
  }

  test("Recommendation for partially specified businessInfo.csv with matcher predictions succeeds"){
    val (newSSD, karmaPredict) = constructKarmaSuggestModel(veryPartialSSD)
    val recommends = karmaPredict
      .suggestModels(
        newSSD, List(exampleOntol), getBusinessDataSetPredictions, businessSemanticTypeMap, businessAttrToColMap2)
    recommends match {
      case Some(ssdPred: SsdPrediction) =>
        assert(ssdPred.suggestions.size === 1)
        val recSemanticModel = ssdPred.suggestions.head._1
        val scores = ssdPred.suggestions.head._2

        val str = compact(Extraction.decompose(recSemanticModel))
        val outputPath = Paths.get(ModelerConfig.KarmaDir, s"recommended_ssd.json")
        // write the object to the file system
        Files.write(
          outputPath,
          str.getBytes(StandardCharsets.UTF_8)
        )

        assert(recSemanticModel.isConsistent)
        assert(recSemanticModel.isComplete)
        assert(scores.nodeCoherence === 1) // all column node mappings are user provided
        assert(scores.nodeConfidence === 1)
        assert(scores.linkCost === 7)
        assert(scores.nodeCoverage === 1)
        assert(recSemanticModel.mappings.isDefined)
        assert(recSemanticModel.mappings.forall(_.mappings.size==4))
      //assert(recSemanticModel.mappings === Some(SSDMapping(Map(4 -> 7, 5 -> 5, 6 -> 4, 7 -> 6)))) // unfortunately, mappings are not fixed
      case _ =>
        fail("Wrong!")
    }
  }

  test("Recommendation for empty getCities.csv succeeds"){
    val (newSSD, karmaPredict) = constructKarmaSuggestModel(emptyCitiesSSD)
    val recommends = karmaPredict
      .suggestModels(newSSD, List(exampleOntol), getCitiesDataSetPredictions, Map(), citiesAttrToColMap)

    recommends match {
      case Some(ssdPred: SsdPrediction) =>
        assert(ssdPred.suggestions.size === 4)
        assert(ssdPred.suggestions.forall(_._1.isComplete)) // Karma should return a consistent and complete semantic model
        assert(ssdPred.suggestions.forall(_._1.isConsistent))
        assert(ssdPred.suggestions.forall(_._1.mappings.isDefined))
        assert(ssdPred.suggestions.forall(_._1.mappings.forall(_.mappings.size==2)))
        assert(ssdPred.suggestions.forall(_._2.nodeConfidence == 0.5))
        assert(ssdPred.suggestions.forall(_._2.nodeCoherence == 1))
        assert(ssdPred.suggestions.forall(_._2.nodeCoverage == 1))

        val recSemanticModel = ssdPred.suggestions.head._1
        val scores = ssdPred.suggestions.head._2
        assert(scores.linkCost === 4)

      case _ =>
        fail("Wrong! There should be some prediction!")
    }
  }

  test("Recommendation for empty getCities.csv with problematic dataset predictions fails"){

    val (newSSD, karmaPredict) = constructKarmaSuggestModel(emptyCitiesSSD)
    val recommends = karmaPredict
      .suggestModels(newSSD,
        List(exampleOntol), getProblematicCitiesDataSetPredictions, Map(), citiesAttrToColMap)

    recommends match {
      case Some(_) =>
        fail("It actually should fail...")
      case _ =>
        succeed
    }
  }

  test("Recommendation for empty getCities.csv with unknown dataset predictions fails"){
    val (newSSD, karmaPredict) = constructKarmaSuggestModel(emptyCitiesSSD)
    val recommends = karmaPredict
      .suggestModels(newSSD,
        List(exampleOntol), getUnknownCitiesDataSetPredictions, Map(), citiesAttrToColMap)

    recommends match {
      case Some(_) =>
        fail("It actually should fail...")
      case _ =>
        succeed
    }
  }

  test("Recommendation for empty getCities.csv with unknown dataset predictions filtered succeeds"){
    val (newSSD, karmaPredict) = constructKarmaSuggestModel(emptyCitiesSSD)

    val convertedDsPreds: Option[DataSetPrediction] = getUnknownCitiesDataSetPredictions match {

      case Some(obj: DataSetPrediction) =>
        logger.debug(s"Semantic Modeler got ${obj.predictions.size} dataset predictions.")
        val filteredPreds: Map[String, ColumnPrediction] =
          PredictOctopus.filterColumnPredictions(obj.predictions, unknownThreshold)
        logger.debug(s"Semantic Modeler will use $filteredPreds")
        assert(filteredPreds.size === 2)
        Some(DataSetPrediction(obj.modelID, obj.dataSetID, filteredPreds))

      case None => None
    }

    val recommends = karmaPredict
      .suggestModels(newSSD,
        List(exampleOntol), convertedDsPreds, Map(), citiesAttrToColMap)

    recommends match {
      case Some(recs) =>
        assert(recs.suggestions.size === 4)
      case None =>
        fail("This should not fail!")
    }
  }

  test("Recommendation for empty getCities.csv with unknown dataset predictions will discard columns"){
    val (newSSD, karmaPredict) = constructKarmaSuggestModel(emptyCitiesSSD)

    val convertedDsPreds: Option[DataSetPrediction] = getDiscardCitiesDataSetPredictions match {

      case Some(obj: DataSetPrediction) =>
        logger.debug(s"Semantic Modeler got ${obj.predictions.size} dataset predictions.")
        val filteredPreds: Map[String, ColumnPrediction] =
          PredictOctopus.filterColumnPredictions(obj.predictions, unknownThreshold)
        logger.debug(s"Semantic Modeler will use $filteredPreds")
        assert(filteredPreds.size === 1)
        Some(DataSetPrediction(obj.modelID, obj.dataSetID, filteredPreds))

      case None => None
    }

    val recommends = karmaPredict
      .suggestModels(newSSD,
        List(exampleOntol), convertedDsPreds, Map(), citiesAttrToColMap)

    recommends match {
      case Some(recs) =>
        assert(recs.suggestions.size === 5)
      case None =>
        fail("This should not fail!")
    }
  }

  test("Recommendation for empty getCities.csv with unknown dataset predictions will reset confidence and label"){
    val (newSSD, karmaPredict) = constructKarmaSuggestModel(emptyCitiesSSD)

    val convertedDsPreds: Option[DataSetPrediction] = getUnknownMaxCitiesDataSetPredictions match {

      case Some(obj: DataSetPrediction) =>
        logger.debug(s"Semantic Modeler got ${obj.predictions.size} dataset predictions.")
        val filteredPreds: Map[String, ColumnPrediction] =
          PredictOctopus.filterColumnPredictions(obj.predictions, unknownThreshold)
        logger.debug(s"Semantic Modeler will use $filteredPreds")
        assert(filteredPreds.size === 2)
        Some(DataSetPrediction(obj.modelID, obj.dataSetID, filteredPreds))

      case None => None
    }

    val recommends = karmaPredict
      .suggestModels(newSSD,
        List(exampleOntol), convertedDsPreds, Map(), citiesAttrToColMap)

    recommends match {
      case Some(recs) =>
        assert(recs.suggestions.size === 4)
      case None =>
        fail("This should not fail!")
    }
  }

  test("Recommendation for empty getCities.csv with filtered problematic dataset predictions will succeed"){
    val (newSSD, karmaPredict) = constructKarmaSuggestModel(emptyCitiesSSD)

    val convertedDsPreds: Option[DataSetPrediction] = getProblematicCitiesDataSetPredictions match {

      case Some(obj: DataSetPrediction) =>
        logger.debug(s"Semantic Modeler got ${obj.predictions.size} dataset predictions.")
        val filteredPreds: Map[String, ColumnPrediction] =
          PredictOctopus.filterColumnPredictions(obj.predictions, unknownThreshold)
        logger.debug(s"Semantic Modeler will use ${filteredPreds.size} ds predictions.")
        Some(DataSetPrediction(obj.modelID, obj.dataSetID, filteredPreds))

      case None => None
    }

    val recommends = karmaPredict
      .suggestModels(newSSD,
        List(exampleOntol), convertedDsPreds, Map(), citiesAttrToColMap)

    recommends match {
      case Some(ssdPred: SsdPrediction) =>
        assert(ssdPred.suggestions.size === 5)
        assert(ssdPred.suggestions.forall(_._1.isComplete)) // Karma should return a consistent and complete semantic model
        assert(ssdPred.suggestions.forall(_._1.isConsistent))
        assert(ssdPred.suggestions.forall(_._1.mappings.isDefined))
        assert(ssdPred.suggestions.forall(_._1.mappings.forall(_.mappings.size == 1)))
        assert(ssdPred.suggestions.forall(_._2.nodeConfidence == 0.5))
        assert(ssdPred.suggestions.forall(_._2.nodeCoherence == 1))
        assert(ssdPred.suggestions.forall(_._2.nodeCoverage == 0.5))

        val recSemanticModel = ssdPred.suggestions.head._1

        val scores = ssdPred.suggestions.head._2
        assert(scores.linkCost === 3)

      case _ =>
        fail("Problems here since there are no recommendations :(")
    }
  }

  test("Recommendation for empty getCities.csv fails due to wrong semantic type map"){
    val (newSSD, karmaPredict) = constructKarmaSuggestModel(emptyCitiesSSD)
    val recommends = karmaPredict
      .suggestModels(newSSD, List(exampleOntol), getCitiesDataSetPredictions, citiesSemanticTypeMap, citiesAttrToColMap)
    assert(recommends.isEmpty)
  }

  test("Recommendation for empty getCities.csv succeeds with correct semantic type map"){
    val (newSSD, karmaPredict) = constructKarmaSuggestModel(emptyCitiesSSD)

    val recommends = karmaPredict
      .suggestModels(
        newSSD, List(exampleOntol), getCitiesDataSetPredictions, correctCitiesSemanticTypeMap, citiesAttrToColMap)
    recommends match {
      case Some(ssdPred: SsdPrediction) =>
        assert(ssdPred.suggestions.size === 4)
        // Karma should return a consistent and complete semantic model
        assert(ssdPred.suggestions.forall(_._1.isComplete))
        assert(ssdPred.suggestions.forall(_._1.isConsistent))
        assert(ssdPred.suggestions.forall(_._1.mappings.isDefined))
        assert(ssdPred.suggestions.forall(_._1.mappings.forall(_.mappings.size==2)))
        assert(ssdPred.suggestions.forall(_._2.nodeConfidence == 0.5))
        assert(ssdPred.suggestions.forall(_._2.nodeCoherence == 1))
        assert(ssdPred.suggestions.forall(_._2.nodeCoverage == 1))

        val scores = ssdPred.suggestions.head._2
        assert(scores.linkCost === 4)

      case _ =>
        fail("Wrong! There should be some prediction!")
    }
  }

  test("Evaluation of cities succeeds"){
    val (newSSD, karmaPredict) = constructKarmaSuggestModel(emptyCitiesSSD)
    val recommends = karmaPredict
      .suggestModels(newSSD, List(exampleOntol), getCitiesDataSetPredictions, Map(), citiesAttrToColMap)

    recommends match {
      case Some(ssdPred: SsdPrediction) =>
        assert(ssdPred.suggestions.size === 4)

        // correct cities ssd
        val correctSSD = readSSD(citiesSSD)

        val evalRes: List[EvaluationResult] = ssdPred.suggestions.map {
          case (sm, ss) =>
            EvaluateOctopus.evaluate(sm, correctSSD, false, false)
        }

        evalRes.foreach(println)

        assert(evalRes.size === ssdPred.suggestions.size)
        // if we consider semantic types, then all scores are pretty small
        assert(evalRes.forall(_.jaccard < 1.0))
        assert(evalRes.forall(_.precision < 1.0))
        assert(evalRes.forall(_.recall < 1.0))

      case _ =>
        fail("Wrong! There should be some prediction!")
    }
  }

  test("Evaluation for businessInfo.csv succeeds"){
    val (newSSD, karmaPredict) = constructKarmaSuggestModel(emptySSD)
    val recommends = karmaPredict
      .suggestModels(
        newSSD, List(exampleOntol), getBusinessDataSetPredictions, businessSemanticTypeMap, businessAttrToColMap2)

    recommends match {
      case Some(ssdPred: SsdPrediction) =>
        assert(ssdPred.suggestions.size === 1)
        // correct business ssd
        val correctSSD = readSSD(exampleSSD)

        val evalRes: List[EvaluationResult] = ssdPred.suggestions.map {
          case (sm, ss) =>
            EvaluateOctopus.evaluate(sm, correctSSD, true, false)
        }

        assert(evalRes.size === ssdPred.suggestions.size)
        // we do not check the correctness of semantic types, but just of the links!
        assert(evalRes.forall(_.jaccard == 1.0))
        assert(evalRes.forall(_.precision == 1.0))
        assert(evalRes.forall(_.recall == 1.0))

      case _ =>
        fail("Wrong! There should be some prediction!")
    }
  }

  test("Evaluation for empty businessInfo.csv fails"){
    // empty business ssd
    val emptyBusinessSSD = readSSD(emptySSD)
    // correct business ssd
    val correctSSD = readSSD(exampleSSD)
    // evaluate both
    Try {
      EvaluateOctopus.evaluate(emptyBusinessSSD, correctSSD, true, false)
    } match {
      case Success(s) =>
        fail("This should have failed!")
      case Failure(err: ModelerException) =>
        succeed
      case Failure(err) =>
        fail(s"Weird error: ${err.getMessage}")
    }
  }

  test("Evaluation for empty businessInfo"){
    addSSD(exampleSSD)
    // empty business ssd
    val emptyBusinessSSD = readSSD(businessEmptySSD)
    // correct business ssd
    val correctSSD = readSSD(exampleSSD)
    // evaluate both
    Try {
      EvaluateOctopus.evaluate(emptyBusinessSSD, correctSSD, false, false)
    } match {
      case Success(s) =>
        // since predicted is empty, all metrics should be 0
        assert(s.jaccard === 0)
        assert(s.precision === 0)
        assert(s.recall === 0)
      case Failure(err) =>
        fail(s"Test failed: ${err.getMessage}")
    }
  }

  test("Evaluation for partial businessInfo"){
    addSSD(exampleSSD)
    // empty business ssd
    val predictedSSD = readSSD(partialSSD)
    // correct business ssd
    val correctSSD = readSSD(exampleSSD)
    // evaluate both
    Try {
      EvaluateOctopus.evaluate(predictedSSD, correctSSD, false, true)
    } match {
      case Success(s) =>
        assert(s.jaccard === 0.57)
        assert(s.precision === 1.0)
        assert(s.recall === 0.57)
      case Failure(err) =>
        fail(s"Test failed: ${err.getMessage}")
    }
  }

  test("Recommendation for empty getCities.csv using personalInfo and businessInfo succeeds"){

    addSSD(exampleSSD)
    addSSD(personSSD)
    logger.info("================================================================")
    val karmaTrain = KarmaBuildAlignmentGraph(karmaWrapper)
    // our alignment
    logger.info("================================================================")
    var alignment = karmaTrain.alignment
    assert(alignment.getGraph.vertexSet.size === 0)
    assert(alignment.getGraph.edgeSet.size === 0)

    logger.info("================================================================")
    alignment = karmaTrain.constructInitialAlignment(knownSSDs)
    assert(alignment.getGraph.vertexSet.size === 9)
    assert(alignment.getGraph.edgeSet.size === 10)

    logger.info("================================================================")
    val newSSD = readSSD(emptyCitiesSSD)
    // check uploaded ontologies....
    assert(karmaWrapper.ontologies.size === 1)
    assert(karmaWrapper.karmaWorkspace.getOntologyManager.getClasses.size === 7)
    assert(karmaWrapper.karmaWorkspace.getOntologyManager.getDataProperties.size === 9)
    assert(karmaWrapper.karmaWorkspace.getOntologyManager.getObjectProperties.size === 12)

    logger.info("================================================================")
    karmaWrapper = KarmaParams(alignmentDir, List(exampleOntol), None)
    val karmaPredict = KarmaSuggestModel(karmaWrapper)
    logger.info("================================================================")

    val recommends = karmaPredict
      .suggestModels(newSSD, List(exampleOntol), getCitiesDataSetPredictions2, Map(), citiesAttrToColMap)

    recommends match {
      case Some(ssdPred: SsdPrediction) =>
        assert(ssdPred.suggestions.size === 10)
        assert(ssdPred.suggestions.forall(_._1.isComplete)) // Karma should return a consistent and complete semantic model
        assert(ssdPred.suggestions.forall(_._1.isConsistent))
        assert(ssdPred.suggestions.forall(_._1.mappings.isDefined))
        assert(ssdPred.suggestions.forall(_._1.mappings.forall(_.mappings.size==2)))

      case _ =>
        fail("Wrong! There should be some prediction!")
    }
  }

}

