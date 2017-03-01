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
import java.nio.file.Paths

import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.scalatest.junit.JUnitRunner

import language.postfixOps
import scala.util.{Failure, Success, Try}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import com.typesafe.scalalogging.LazyLogging

import scalax.collection.Graph
import au.csiro.data61.types._
import au.csiro.data61.types.ColumnTypes.ColumnID
import org.joda.time.DateTime


/**
  * Tests for the Semantic Source Description
  */

@RunWith(classOf[JUnitRunner])
class SemanticSourceSpec  extends FunSuite with ModelerJsonFormats with BeforeAndAfterEach with LazyLogging {

  val dummySsdID = 1

  val ssdDir = getClass.getResource("/ssd").getPath

  def emptySSD: String = Paths.get(ssdDir,"empty_model.ssd") toString
  def exampleSSD: String = Paths.get(ssdDir,"businessInfo.ssd") toString
  def mapJson: String = Paths.get(ssdDir,"mappings_sample.json") toString

  def dummyGraph: Graph[SSDNode, SSDLink] = {
    val ssdLab1: SSDLabel = SSDLabel("Person", "ClassNode")
    val ssdLab2: SSDLabel = SSDLabel("name", "DataNode")
    val n1: SSDNode = SSDNode(1,ssdLab1)
    val n2: SSDNode = SSDNode(2,ssdLab2)
    val linkLab: SSDLabel = SSDLabel("name","DataProperty")
    Graph(SSDLink(n1,n2,1,linkLab))
  }

  def dummyCol: SsdColumn = SsdColumn(1, "ceo")
  def dummyAttr: SsdAttribute = SsdAttribute(1, "ceo", "ident", List(1), "select ceo from 'businessInfo.csv'")
  def dummyAttr2: SsdAttribute = SsdAttribute(1, "ceo", "ident", List(2), "select ceo from 'businessInfo.csv'")
  def dummyOntol: String = "/home/rue009/Projects/DFAT/SchemaMapping/Transformations/tests/example/dataintegration_report_ontology.owl"
  def dummyMap: SsdMapping = SsdMapping(Map(1 -> 2))
  def dummyMap2: SsdMapping = SsdMapping(Map(1 -> 2, 1 -> 4))
  def dummyMap3: SsdMapping = SsdMapping(Map(1 -> 2, 3 -> 1))


  /**
    * Get the list of ssd nodes from the semantic source description
    * @param ssd Semantic Source Description
    * @return
    */
  def getSMNodes(ssd: SemanticSourceDesc): List[SSDNode] = {
    ssd.semanticModel match {
      case Some(sm) => sm.getNodes
      case None => List()
    }
  }

  /**
    * Get the list of ssd links from the semantic source description
    * @param ssd Semantic Source Description
    * @return
    */
  def getSMLinks(ssd: SemanticSourceDesc): List[SSDLink[SSDNode]] = {
    ssd.semanticModel match {
      case Some(sm) => sm.getLinks.map(e => e.asInstanceOf[SSDLink[SSDNode]])
      case None => List()
    }
  }

  /**
    * Amount of mappings in the ssd.
    * @param ssd Semantic Source Description
    * @return
    */
  def getMappingSize(ssd: SemanticSourceDesc): Int = {
    ssd.mappings match {
      case Some(maps) => maps.mappings.size
      case None => 0
    }
  }

  test("Successful creation of SSD"){
    val ssd = SemanticSourceDesc(
      name = "test",
      id = dummySsdID,
      attributes = List(dummyAttr),
      ontology = List(1),
      semanticModel = Some(SemanticModel(dummyGraph)),
      mappings = Some(dummyMap),
      dateCreated = DateTime.now,
      dateModified = DateTime.now)

    assert(ssd.isConsistent)
    assert(ssd.isComplete)
  }

  test("Inconsistent SSD: attributes are inconsistent") {
    val ssd = SemanticSourceDesc(
      name = "test",
      id = dummySsdID,
      attributes = List(dummyAttr, dummyAttr2), // attributes are inconsistent
      ontology = List(1),
      semanticModel = Some(SemanticModel(dummyGraph)),
      mappings = Some(dummyMap),
      dateCreated = DateTime.now,
      dateModified = DateTime.now)

    assert(!ssd.isConsistent)
    assert(!ssd.isComplete)
  }

  test("Inconsistent SSD: mappings are inconsistent") {
    val ssd2 = SemanticSourceDesc(
      name = "test",
      id = dummySsdID,
      attributes = List(dummyAttr),
      ontology = List(1),
      semanticModel = Some(SemanticModel(dummyGraph)),
      mappings = Some(dummyMap2),
      dateCreated = DateTime.now,
      dateModified = DateTime.now) // mappings are inconsistent

    assert(!ssd2.isConsistent)
    assert(!ssd2.isComplete)
  }

  test("Inconsistent SSD: mappings are again inconsistent") {
    val ssd3 = SemanticSourceDesc(
      name = "test",
      id = dummySsdID,
      attributes = List(dummyAttr),
      ontology = List(1),
      semanticModel = Some(SemanticModel(dummyGraph)),
      mappings = Some(dummyMap3),
      dateCreated = DateTime.now,
      dateModified = DateTime.now) // mappings are inconsistent

    assert(!ssd3.isConsistent)
    assert(!ssd3.isComplete)
  }

  test("Inconsistent SSD: semantic model - mappings clash") {
    val ssd4 = SemanticSourceDesc(
      name = "test",
      id = dummySsdID,
      attributes = List(dummyAttr),
      ontology = List(1),
      semanticModel = Some(SemanticModel(Graph())), // semantic model is absent while mappings are there
      mappings = Some(dummyMap),
      dateCreated = DateTime.now,
      dateModified = DateTime.now)

    assert(!ssd4.isConsistent)
    assert(!ssd4.isComplete)
  }

  test("Jsonify SSD"){
    val ssd = SemanticSourceDesc(
      name = "test",
      id = dummySsdID,
      attributes = List(dummyAttr),
      ontology = List(1),
      semanticModel = Some(SemanticModel(dummyGraph)),
      mappings = Some(dummyMap),
      dateCreated = DateTime.now,
      dateModified = DateTime.now)
    val json = Extraction.decompose(ssd)
    print(json)
    val ssd2 = json.extract[SemanticSourceDesc]

    assert(ssd.mappings === ssd2.mappings)
    assert(ssd.attributes === ssd2.attributes)
    assert(ssd.ontology === ssd2.ontology)
    assert(getSMNodes(ssd) === getSMNodes(ssd2))
    assert(getSMLinks(ssd) === getSMLinks(ssd2))
    assert(ssd.semanticModel === ssd2.semanticModel)
    assert(ssd.dateModified === ssd2.dateModified)
    assert(ssd.dateCreated === ssd2.dateCreated)
    assert(ssd === ssd2)
  }

  test("Read mappings from json"){
    Try {
      val stream = new FileInputStream(Paths.get(mapJson).toFile)
      parse(stream).extract[SsdMapping]
    } match {
      case Success(res) =>
        assert(res.mappings.size === 4)
      case Failure(err) =>
        fail(err.getMessage)
    }
  }

  test("Read example ssd from file"){
    Try {
      val stream = new FileInputStream(Paths.get(exampleSSD).toFile)
      parse(stream).extract[SemanticSourceDesc]
    } match {
      case Success(ssd) =>

        assert(ssd.name === "businessInfo.csv")
        assert(ssd.attributes.size === 4)
        assert(getMappingSize(ssd) === 4)
        assert(getSMNodes(ssd).size === 8)
        assert(getSMLinks(ssd).size === 7)
        assert(ssd.isConsistent)
        assert(ssd.isComplete)
      case Failure(err) =>
        fail(err.getMessage)
    }
  }

  test("Read empty ssd from file"){
    Try {
      val stream = new FileInputStream(Paths.get(emptySSD).toFile)
      parse(stream).extract[SemanticSourceDesc]
    } match {
      case Success(ssd) =>
        assert(ssd.name === "businessInfo.csv")
        assert(ssd.attributes.size === 4)
        assert(getMappingSize(ssd) === 0)
        assert(getSMNodes(ssd).size === 0)
        assert(getSMLinks(ssd).size === 0)
        assert(ssd.isConsistent)
        assert(!ssd.isComplete)
      case Failure(err) =>
        fail(err.getMessage)
    }
  }
}
