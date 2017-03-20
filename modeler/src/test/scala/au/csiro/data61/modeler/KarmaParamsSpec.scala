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

import java.io
import java.io.FileInputStream
import java.nio.file.{Path, Paths}

import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.scalatest.junit.JUnitRunner

import language.postfixOps
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import com.typesafe.scalalogging.LazyLogging
import au.csiro.data61.types._
import au.csiro.data61.modeler.karma.KarmaParams
import au.csiro.data61.types.Exceptions.ModelerException
import edu.isi.karma.rep.alignment.ColumnNode
import edu.isi.karma.webserver.ServletContextParameterMap.ContextParameter

/**
  * Tests for the Semantic Source Description
  */

@RunWith(classOf[JUnitRunner])
class KarmaParamsSpec extends FunSuite with ModelerJsonFormats with BeforeAndAfterEach with LazyLogging {

  val ssdDir = getClass.getResource("/ssd").getPath
  val karmaDir = getClass.getResource("/karma").getPath
  val alignmentDir = Paths.get("/tmp/test-ssd", "alignment") toString
  def exampleSSD: String = Paths.get(ssdDir,"businessInfo.ssd") toString
  def exampleKarmaSSD: String = Paths.get(karmaDir,"businessInfo.csv.model.json") toString
  def exampleSM: String = Paths.get(ssdDir,"semantic_model_example.json") toString
  def exampleOntol: String = Paths.get(ssdDir,"dataintegration_report_ontology.ttl") toString

  val dummyOwlID = 1
  val dummySsdID = 1

  var knownSSDs: List[Ssd] = List()
  var karmaWrapper = KarmaParams(alignmentDir, List(), None)

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

  override def beforeEach(): Unit = {
    Try {
      val stream = new FileInputStream(Paths.get(exampleSSD).toFile)
      parse(stream).extract[Ssd]
    } match {
      case Success(ssd) =>
        knownSSDs = List(ssd)
      case Failure(err) =>
        fail(err.getMessage)
    }
    karmaWrapper = KarmaParams(alignmentDir, List(exampleOntol), None)
  }

  override def afterEach(): Unit = {
    knownSSDs = List()
    karmaWrapper.deleteKarma()
    // we need to clean the alignmentDir
    removeAll(Paths.get(alignmentDir))

  }

  /**
    * Get the list of ssd nodes from the semantic source description
 *
    * @param ssd Semantic Source Description
    * @return
    */
  def getSMNodes(ssd: Ssd): List[SsdNode] = {
    ssd.semanticModel match {
      case Some(sm) => sm.getNodes
      case None => List()
    }
  }

  /**
    * Get the list of ssd links from the semantic source description
 *
    * @param ssd Semantic Source Description
    * @return
    */
  def getSMLinks(ssd: Ssd): List[SsdLink[SsdNode]] = {
    ssd.semanticModel match {
      case Some(sm) => sm.getLinks.map(e => e.asInstanceOf[SsdLink[SsdNode]])
      case None => List()
    }
  }

  /**
    * Get the list of ssd node labels from the semantic source description
 *
    * @param ssd Semantic Source Description
    * @return
    */
  def getSMNodeLabels(ssd: Ssd): List[SsdLabel] = {
    ssd.semanticModel match {
      case Some(sm) => sm.getNodeLabels
      case None => List()
    }
  }

  /**
    * Get the list of ssd link labels from the semantic source description
 *
    * @param ssd Semantic Source Description
    * @return
    */
  def getSMLinkLabels(ssd: Ssd): List[(SsdLabel, SsdLabel, SsdLabel)] = {
    ssd.semanticModel match {
      case Some(sm) => sm.getLinkLabels
      case None => List()
    }
  }

  /**
    * Amount of mappings in the ssd.
 *
    * @param ssd Semantic Source Description
    * @return
    */
  def getMappingSize(ssd: Ssd): Int = {
    ssd.mappings match {
      case Some(maps) => maps.mappings.size
      case None => 0
    }
  }

  test("Karma tool initialization"){
    assert(Paths.get(karmaWrapper.karmaContextParameters.getParameterValue(ContextParameter.USER_DIRECTORY_PATH))
      === Paths.get(ModelerConfig.KarmaDir))
    // randomly checking one of the values... which can randomly change :)
    assert(karmaWrapper.karmaModelingConfiguration.getDefaultProperty === "http://schema.org/name")

    // check uploaded ontologies....
    assert(karmaWrapper.karmaWorkspace.getOntologyManager.getClasses.size === 7)
    assert(karmaWrapper.karmaWorkspace.getOntologyManager.getDataProperties.size === 9)
    assert(karmaWrapper.karmaWorkspace.getOntologyManager.getObjectProperties.size === 12)
    }

  test("Prefix map") {
    assert(karmaWrapper.prefixMap.size === 5)
    assert(karmaWrapper.prefixMap.get("serene-default") === Some(TypeConfig.DefaultNamespace))
  }

  test("Successful conversion of Karma Graph to our SemanticModel") {
    val karmaModel = karmaWrapper.readKarmaModelJson(exampleKarmaSSD)
    val ourSemModel = karmaModel.karmaSM.toSemanticModel
    assert(ourSemModel.isConnected === true)
    assert(ourSemModel.getNodes.size === 8)
    assert(ourSemModel.getLinks.size === 7)
  }

  test("Successful conversion of KarmaSemanticModel to our SemantciSourceDesc") {
    assert(knownSSDs.flatMap(_.ontologies).size === 1)
    assert(karmaWrapper.ontologies.size === 1)
    val model = karmaWrapper.readKarmaModelJson(exampleKarmaSSD)
    val ssd = model.toSSD(dummySsdID, TypeConfig.SSDVersion, List(dummyOwlID), "businessInfo.csv")

    assert(ssd.isComplete === true)
    assert(ssd.isConsistent === true)
    assert(ssd.ontologies.size === 1)
    assert(getSMNodes(ssd).size === 8)
    assert(getSMLinks(ssd).size === 7)
    assert(ssd.attributes.size === 4)
  }

  test("Successful conversion of converted SemanticModel to Karma Graph") {
    val model = karmaWrapper.readKarmaModelJson(exampleKarmaSSD)
    val ssd = model.toSSD(dummySsdID, TypeConfig.SSDVersion, List(dummyOwlID), "businessInfo.csv")
    val semModel = model.karmaSM.toSemanticModel
    val karmaGraph = ssd.mappings match {
      case Some(maps) =>
        semModel.toKarmaGraph(maps, karmaWrapper.karmaWorkspace.getOntologyManager)
      case None =>
        fail("SSD mappings are empty.")
    }
    val convertedSemModel = karmaGraph.toSemanticModel

    assert(convertedSemModel.getNodeLabels === semModel.getNodeLabels)
    assert(convertedSemModel.getLinkLabels === semModel.getLinkLabels)
  }

  test("Successful conversion of example SemanticModel to Karma Graph") {
    val ssd: Ssd = knownSSDs.headOption match {
      case Some(s: Ssd) => s
      case _ => fail("SSD should be in the Storage!")
    }

    val semModel = ssd.semanticModel match {
      case Some(sm) => sm
      case None => fail("Semantic model is missing.")
    }
    val karmaGraph = ssd.mappings match {
      case Some(maps) =>
        semModel.toKarmaGraph(maps, karmaWrapper.karmaWorkspace.getOntologyManager)
      case None =>
        fail("Mappings are missing in SSD.")
    }

    karmaGraph.graph.vertexSet.asScala.toList.foreach{
      case node: ColumnNode =>
        assert(node.isForced === true)
        assert(node.getRdfLiteralType === null)
      case node =>
        assert(node.isForced === true)
        assert(node.getLabel != null)
    }

    val convertedSemModel = karmaGraph.toSemanticModel

    assert(convertedSemModel.getNodeLabels === semModel.getNodeLabels)
    assert(convertedSemModel.getLinkLabels === semModel.getLinkLabels)
  }

  test("Successful conversion of converted SemantciSourceDesc to KarmaSemanticModel") {
    val model = karmaWrapper.readKarmaModelJson(exampleKarmaSSD)
    val ssd: Ssd = model.toSSD(dummySsdID, TypeConfig.SSDVersion, List(dummyOwlID), "businessInfo.csv")

    val karmaConversion = ssd.toKarmaSemanticModel(karmaWrapper.karmaWorkspace.getOntologyManager)
    val ssdConversion: Ssd = karmaConversion match {
      case Some(converted) => converted.toSSD(dummySsdID, TypeConfig.SSDVersion, List(dummyOwlID), "businessInfo.csv")
      case None =>
        fail("Converting to Karma Semantic model failed.")
    }

    assert(getSMNodeLabels(ssd) === getSMNodeLabels(ssdConversion))
    assert(getSMLinkLabels(ssd) === getSMLinkLabels(ssdConversion))
    assert(ssd.ontologies === ssdConversion.ontologies)
    assert(ssd.attributes.map(_.id).toSet === ssdConversion.attributes.map(_.id).toSet)
    assert(ssd.id === ssdConversion.id)
    assert(getMappingSize(ssd) === getMappingSize(ssdConversion))
  }

  test("Successful conversion of example SemanticSourceDesc to KarmaSemanticModel") {
    val ssd: Ssd = knownSSDs.headOption match {
      case Some(s: Ssd) => s
      case _ => fail("SSD should be in the Storage!")
    }

    val karmaConversion = ssd.toKarmaSemanticModel(karmaWrapper.karmaWorkspace.getOntologyManager)
    val ssdConversion: Ssd = karmaConversion match {
      case Some(converted) => converted.toSSD(ssd.id, TypeConfig.SSDVersion, List(dummyOwlID), ssd.name)
      case None =>
        fail("Converting to Karma Semantic model failed.")
    }

    assert(getSMNodeLabels(ssd) === getSMNodeLabels(ssdConversion))
    assert(getSMLinkLabels(ssd) === getSMLinkLabels(ssdConversion))
    assert(ssd.ontologies.size === ssdConversion.ontologies.size)
    assert(ssd.attributes.map(_.id).toSet === ssdConversion.attributes.map(_.id).toSet)
    assert(ssd.id === ssdConversion.id)
    assert(getMappingSize(ssd) === getMappingSize(ssdConversion))
  }
}

