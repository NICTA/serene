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
import au.csiro.data61.types.SSDTypes._

/**
  * Created by natalia on 14/11/16.
  */
@RunWith(classOf[JUnitRunner])
class SuggestModelSpec  extends FunSuite with ModelerJsonFormats with BeforeAndAfterEach with LazyLogging {

  val ssdDir = getClass.getResource("/ssd").getPath
  val exampleSSD: String = Paths.get(ssdDir,"businessInfo.ssd") toString
  val emptySSD: String = Paths.get(ssdDir,"empty_model_2.ssd") toString
  val partialSSD: String = Paths.get(ssdDir,"partial_model.ssd") toString
  val veryPartialSSD: String = Paths.get(ssdDir,"partial_model2.ssd") toString
  val emptyCitiesSSD: String = Paths.get(ssdDir,"empty_getCities.ssd") toString

  val alignmentDir = Paths.get("/tmp/test-ssd", "alignment") toString
  val exampleOntol: String = Paths.get(ssdDir,"dataintegration_report_ontology.owl") toString

  var knownSSDs: List[SemanticSourceDesc] = List()
  var karmaWrapper = KarmaParams(alignmentDir, List(), None)

  def addSSD(ssdPath: String): Unit = {
    Try {
      val stream = new FileInputStream(Paths.get(ssdPath).toFile)
      parse(stream).extract[SemanticSourceDesc]
    } match {
      case Success(ssd) =>
        knownSSDs = ssd :: knownSSDs
      case Failure(err) =>
        fail(err.getMessage)
    }
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
    karmaWrapper.deleteKarma()
    // we need to clean the alignmentDir
    removeAll(Paths.get(alignmentDir))
  }

  /**
    * Construct DataSetPrediction instance for businessInfo.csv
    * @return
    */
  def getBusinessDataSetPredictions: Option[DataSetPrediction] = {
    // we care only about scores in the semantic-modeller
    val col0 = ColumnPrediction(label="Organization#name",
      confidence=0.5,
      scores=Map("Organization#name" -> 0.5),
      features=Map())
    val col1 = ColumnPrediction(label="Person#name",
      confidence=1.0,
      scores=Map("Person#name" -> 1.0),
      features=Map())
    val col2 = ColumnPrediction(label="City#name",
      confidence=1.0,
      scores=Map("City#name" -> 1.0),
      features=Map())
    val col3 = ColumnPrediction(label="State#name",
      confidence=1.0,
      scores=Map("State#name" -> 1.0),
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
    * @return
    */
  def getCitiesDataSetPredictions: Option[DataSetPrediction] = {
    // we care only about scores in the semantic-modeller
    val col0 = ColumnPrediction(label="City#name",
      confidence=0.5,
      scores=Map("City#name" -> 0.5, "State#name" -> 0.5),
      features=Map())
    val col1 = ColumnPrediction(label="City#name",
      confidence=0.5,
      scores=Map("City#name" -> 0.5, "State#name" -> 0.5),
      features=Map())

    Some(DataSetPrediction(
      modelID = 0,
      dataSetID = 0,
      predictions = Map("10" -> col0, "11" -> col1)
    ))
  }

  // wrong semantic type map
  val citiesSemanticTypeMap: Map[String, String] = Map(
    "City" -> "abc:City"
    , "name" -> "abc:name"
    , "State" -> "abc:State"
  )
  // correct semantic type map
  val correctCitiesSemanticTypeMap: Map[String, String] = Map(
    "City" -> "http://www.semanticweb.org/data_integration_project/report_example_ontology#"
    , "name" -> "http://www.semanticweb.org/data_integration_project/report_example_ontology#"
    , "State" -> "http://www.semanticweb.org/data_integration_project/report_example_ontology#"
  )

  val citiesAttrToColMap: Map[AttrID,ColumnID] = Map(10 -> 10, 11 -> 11)

  test("Recommendation for businessInfo.csv fails since there are no preloaded ontologies"){
    val newSSD = Try {
      val stream = new FileInputStream(Paths.get(emptySSD).toFile)
      parse(stream).extract[SemanticSourceDesc]
    } match {
      case Success(ssd) =>
        ssd
      case Failure(err) =>
        fail(err.getMessage)
    }

    // check uploaded ontologies....
    assert(karmaWrapper.ontologies.size === 0)
    assert(karmaWrapper.karmaWorkspace.getOntologyManager.getClasses.size != 7)
    assert(karmaWrapper.karmaWorkspace.getOntologyManager.getDataProperties.size != 9)
    assert(karmaWrapper.karmaWorkspace.getOntologyManager.getObjectProperties.size != 12)

    val karmaPredict = KarmaSuggestModel(karmaWrapper)
    val recommends = karmaPredict
      .suggestModels(newSSD, List(exampleOntol), getBusinessDataSetPredictions, businessSemanticTypeMap, businessAttrToColMap)
    assert(recommends.isEmpty)
  }

  test("Recommendation for businessInfo.csv fails since the alignment graph is not constructed"){
    addSSD(exampleSSD)
    val newSSD = Try {
      val stream = new FileInputStream(Paths.get(emptySSD).toFile)
      parse(stream).extract[SemanticSourceDesc]
    } match {
      case Success(ssd) =>
        ssd
      case Failure(err) =>
        fail(err.getMessage)
    }
    // check uploaded ontologies....
    assert(karmaWrapper.ontologies.size === 1)
    assert(karmaWrapper.karmaWorkspace.getOntologyManager.getClasses.size === 7)
    assert(karmaWrapper.karmaWorkspace.getOntologyManager.getDataProperties.size === 9)
    assert(karmaWrapper.karmaWorkspace.getOntologyManager.getObjectProperties.size === 12)

    val karmaPredict = KarmaSuggestModel(karmaWrapper)
    val recommends = karmaPredict
      .suggestModels(newSSD, List(exampleOntol), getBusinessDataSetPredictions, businessSemanticTypeMap, businessAttrToColMap)
    assert(recommends.isEmpty)
  }

  test("Recommendation for empty businessInfo.csv succeeds"){
    addSSD(exampleSSD)
    // first, we build the Alignment Graph = training step
    val karmaTrain = KarmaBuildAlignmentGraph(karmaWrapper)
    // our alignment
    var alignment = karmaTrain.alignment
    assert(alignment.getGraph.vertexSet.size === 0)
    assert(alignment.getGraph.edgeSet.size === 0)

    alignment = karmaTrain.constructInitialAlignment(knownSSDs)
    assert(alignment.getGraph.vertexSet.size === 8)
    assert(alignment.getGraph.edgeSet.size === 7)

    val newSSD = Try {
      val stream = new FileInputStream(Paths.get(emptySSD).toFile)
      parse(stream).extract[SemanticSourceDesc]
    } match {
      case Success(ssd) =>
        ssd
      case Failure(err) =>
        fail(err.getMessage)
    }
    // check uploaded ontologies....
    assert(karmaWrapper.ontologies.size === 1)
    assert(karmaWrapper.karmaWorkspace.getOntologyManager.getClasses.size === 7)
    assert(karmaWrapper.karmaWorkspace.getOntologyManager.getDataProperties.size === 9)
    assert(karmaWrapper.karmaWorkspace.getOntologyManager.getObjectProperties.size === 12)

    // now, we run prediction for the new SSD
    karmaWrapper = KarmaParams(alignmentDir, List(exampleOntol), None)
    val karmaPredict = KarmaSuggestModel(karmaWrapper)
    val recommends = karmaPredict
      .suggestModels(newSSD, List(exampleOntol), getBusinessDataSetPredictions, businessSemanticTypeMap, businessAttrToColMap2)

    recommends match {
      case Some(ssdPred: SSDPrediction) =>
        assert(ssdPred.suggestions.size === 1)
        val recSemanticModel = ssdPred.suggestions(0)._1
        val scores = ssdPred.suggestions(0)._2

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
    val newSSD = Try {
      val stream = new FileInputStream(Paths.get(partialSSD).toFile)
      parse(stream).extract[SemanticSourceDesc]
    } match {
      case Success(ssd) =>
        ssd
      case Failure(err) =>
        fail(err.getMessage)
    }
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
      .suggestModels(newSSD, List(exampleOntol), None, businessSemanticTypeMap, businessAttrToColMap2)
    recommends match {
      case Some(ssdPred: SSDPrediction) =>
        assert(ssdPred.suggestions.size === 1)
        val recSemanticModel = ssdPred.suggestions(0)._1
        val scores = ssdPred.suggestions(0)._2

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
    val newSSD = Try {
      val stream = new FileInputStream(Paths.get(veryPartialSSD).toFile)
      parse(stream).extract[SemanticSourceDesc]
    } match {
      case Success(ssd) =>
        ssd
      case Failure(err) =>
        fail(err.getMessage)
    }
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
      .suggestModels(newSSD, List(exampleOntol), getBusinessDataSetPredictions, businessSemanticTypeMap, businessAttrToColMap2)
    recommends match {
      case Some(ssdPred: SSDPrediction) =>
        assert(ssdPred.suggestions.size === 1)
        val recSemanticModel = ssdPred.suggestions(0)._1
        val scores = ssdPred.suggestions(0)._2

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
    addSSD(exampleSSD)
    // first, we build the Alignment Graph = training step
    val karmaTrain = KarmaBuildAlignmentGraph(karmaWrapper)
    // our alignment
    var alignment = karmaTrain.alignment
    assert(alignment.getGraph.vertexSet.size === 0)
    assert(alignment.getGraph.edgeSet.size === 0)

    alignment = karmaTrain.constructInitialAlignment(knownSSDs)
    assert(alignment.getGraph.vertexSet.size === 8)
    assert(alignment.getGraph.edgeSet.size === 7)

    val newSSD = Try {
      val stream = new FileInputStream(Paths.get(emptyCitiesSSD).toFile)
      parse(stream).extract[SemanticSourceDesc]
    } match {
      case Success(ssd) =>
        ssd
      case Failure(err) =>
        fail(err.getMessage)
    }
    // check uploaded ontologies....
    assert(karmaWrapper.ontologies.size === 1)
    assert(karmaWrapper.karmaWorkspace.getOntologyManager.getClasses.size === 7)
    assert(karmaWrapper.karmaWorkspace.getOntologyManager.getDataProperties.size === 9)
    assert(karmaWrapper.karmaWorkspace.getOntologyManager.getObjectProperties.size === 12)

    // now, we run prediction for the new SSD
    karmaWrapper = KarmaParams(alignmentDir, List(exampleOntol), None)
    val karmaPredict = KarmaSuggestModel(karmaWrapper)
    val recommends = karmaPredict
      .suggestModels(newSSD, List(exampleOntol), getCitiesDataSetPredictions, Map(), citiesAttrToColMap)

    recommends match {
      case Some(ssdPred: SSDPrediction) =>
        assert(ssdPred.suggestions.size === 4)
        assert(ssdPred.suggestions.forall(_._1.isComplete)) // Karma should return a consistent and complete semantic model
        assert(ssdPred.suggestions.forall(_._1.isConsistent))
        assert(ssdPred.suggestions.forall(_._1.mappings.isDefined))
        assert(ssdPred.suggestions.forall(_._1.mappings.forall(_.mappings.size==2)))
        assert(ssdPred.suggestions.forall(_._2.nodeConfidence == 0.5))
        assert(ssdPred.suggestions.forall(_._2.nodeCoherence == 1))
        assert(ssdPred.suggestions.forall(_._2.nodeCoverage == 1))

        ssdPred.suggestions.forall(_._1.mappings.isDefined)

        val recSemanticModel = ssdPred.suggestions(0)._1
        val scores = ssdPred.suggestions(0)._2
        assert(scores.linkCost === 4)

      case _ =>
        fail("Wrong! There should be some prediction!")
    }
  }

  test("Recommendation for empty getCities.csv fails due to wrong semantic type map"){
    addSSD(exampleSSD)
    // first, we build the Alignment Graph = training step
    val karmaTrain = KarmaBuildAlignmentGraph(karmaWrapper)
    // our alignment
    var alignment = karmaTrain.alignment
    assert(alignment.getGraph.vertexSet.size === 0)
    assert(alignment.getGraph.edgeSet.size === 0)

    alignment = karmaTrain.constructInitialAlignment(knownSSDs)
    assert(alignment.getGraph.vertexSet.size === 8)
    assert(alignment.getGraph.edgeSet.size === 7)

    val newSSD = Try {
      val stream = new FileInputStream(Paths.get(emptyCitiesSSD).toFile)
      parse(stream).extract[SemanticSourceDesc]
    } match {
      case Success(ssd) =>
        ssd
      case Failure(err) =>
        fail(err.getMessage)
    }
    // check uploaded ontologies....
    assert(karmaWrapper.ontologies.size === 1)
    assert(karmaWrapper.karmaWorkspace.getOntologyManager.getClasses.size === 7)
    assert(karmaWrapper.karmaWorkspace.getOntologyManager.getDataProperties.size === 9)
    assert(karmaWrapper.karmaWorkspace.getOntologyManager.getObjectProperties.size === 12)

    // now, we run prediction for the new SSD
    karmaWrapper = KarmaParams(alignmentDir, List(exampleOntol), None)
    val karmaPredict = KarmaSuggestModel(karmaWrapper)
    val recommends = karmaPredict
      .suggestModels(newSSD, List(exampleOntol), getCitiesDataSetPredictions, citiesSemanticTypeMap, citiesAttrToColMap)
    assert(recommends.isEmpty)
  }

  test("Recommendation for empty getCities.csv succeeds with correct semantic type map"){
    addSSD(exampleSSD)
    // first, we build the Alignment Graph = training step
    val karmaTrain = KarmaBuildAlignmentGraph(karmaWrapper)
    // our alignment
    var alignment = karmaTrain.alignment
    assert(alignment.getGraph.vertexSet.size === 0)
    assert(alignment.getGraph.edgeSet.size === 0)

    alignment = karmaTrain.constructInitialAlignment(knownSSDs)
    assert(alignment.getGraph.vertexSet.size === 8)
    assert(alignment.getGraph.edgeSet.size === 7)

    val newSSD = Try {
      val stream = new FileInputStream(Paths.get(emptyCitiesSSD).toFile)
      parse(stream).extract[SemanticSourceDesc]
    } match {
      case Success(ssd) =>
        ssd
      case Failure(err) =>
        fail(err.getMessage)
    }
    // check uploaded ontologies....
    assert(karmaWrapper.ontologies.size === 1)
    assert(karmaWrapper.karmaWorkspace.getOntologyManager.getClasses.size === 7)
    assert(karmaWrapper.karmaWorkspace.getOntologyManager.getDataProperties.size === 9)
    assert(karmaWrapper.karmaWorkspace.getOntologyManager.getObjectProperties.size === 12)

    // now, we run prediction for the new SSD
    karmaWrapper = KarmaParams(alignmentDir, List(exampleOntol), None)
    val karmaPredict = KarmaSuggestModel(karmaWrapper)
    val recommends = karmaPredict
      .suggestModels(newSSD, List(exampleOntol), getCitiesDataSetPredictions, correctCitiesSemanticTypeMap, citiesAttrToColMap)
    recommends match {
      case Some(ssdPred: SSDPrediction) =>
        assert(ssdPred.suggestions.size === 4)
        // Karma should return a consistent and complete semantic model
        assert(ssdPred.suggestions.forall(_._1.isComplete))
        assert(ssdPred.suggestions.forall(_._1.isConsistent))
        assert(ssdPred.suggestions.forall(_._1.mappings.isDefined))
        assert(ssdPred.suggestions.forall(_._1.mappings.forall(_.mappings.size==2)))
        assert(ssdPred.suggestions.forall(_._2.nodeConfidence == 0.5))
        assert(ssdPred.suggestions.forall(_._2.nodeCoherence == 1))
        assert(ssdPred.suggestions.forall(_._2.nodeCoverage == 1))

        val recSemanticModel = ssdPred.suggestions(0)._1
        val scores = ssdPred.suggestions(0)._2
        assert(scores.linkCost === 4)

      case _ =>
        fail("Wrong! There should be some prediction!")
    }
  }
}

