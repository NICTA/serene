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

import java.io.FileInputStream
import java.nio.file.Paths

import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.scalatest.junit.JUnitRunner

import language.postfixOps
import scala.util.{Failure, Success, Try}
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scalax.collection.Graph
import scalax.collection.GraphPredef._
import au.csiro.data61.types._
import au.csiro.data61.types.SsdLink.ImplicitEdge // needs to be explicitly imported to use shortcut ##

/**
  * Tests for the Semantic Model and scala-graph library
  */

@RunWith(classOf[JUnitRunner])
class SemanticModelSpec extends FunSuite with ModelerJsonFormats with BeforeAndAfterEach {

  val ssdDir = getClass.getResource("/ssd").getPath

  def emptySM: String = Paths.get(ssdDir,"empty_semantic_model.json") toString
  def exampleSM: String = Paths.get(ssdDir,"semantic_model_example.json") toString

  def ssdLab1: SsdLabel = SsdLabel("Person", "ClassNode", "ForcedByUser")
  def ssdLab2: SsdLabel = SsdLabel("name", "DataNode", "ForcedByUser")
  def n1: SsdNode = SsdNode(1,ssdLab1)
  def n2: SsdNode = SsdNode(2,ssdLab2)
  def linkLab: SsdLabel = SsdLabel("name","DataProperty","ForcedByUser")
  def linkLab2: SsdLabel = SsdLabel("name2","DataProperty","ForcedByUser")
  def outer: SsdLink[SsdNode] = SsdLink(n1,n2,1,linkLab)

  def dummyGraph: Graph[SsdNode, SsdLink] = Graph(outer)

  test("Successfully created dummy scala graph") {
    val g = dummyGraph
    val e  = g.edges.head

    assert(e.edge.nodes.productElement(0).asInstanceOf[AnyRef].getClass === g.nodes.head.getClass)
    assert(e.from === n1)
    assert(e.to === n2)
    assert(e.id === 1)
    assert(e.## === outer.##)
    assert(e.from === outer.from)
    assert(e.to === outer.to)
    assert(e.ssdLabel === outer.ssdLabel)
    assert(e === outer)
    val linkLab2 = SsdLabel("name2","DataProperty","ForcedByUser")
    val equalLink = SsdLink(n1, n2, 1, linkLab2)
    assert(e === equalLink)
    assert(e.## === equalLink.##)
    val neLink = SsdLink(n1, n2, 2, linkLab2)
    assert(e != neLink)
    assert(e.## != neLink.##)
  }

  test("Successful link addition to dummy graph") {
    val g = dummyGraph

//    val link2: SSDLink[SSDNode] = n1~>n2 ## (2,linkLab2)
    val g2 = g + n1~>n2 ##(2,linkLab2)

    assert(g.nodes.size === 2)
    assert(g.nodes.size === g2.nodes.size)
    assert(g.edges.size === g2.edges.size - 1)
  }

  test("Successful modification of a mutable scala graph") {
    val g = scalax.collection.mutable.Graph(outer)

//    val link2: SSDLink[SSDNode] = n1~>n2 ## (2,linkLab2)
    g += n1~>n2 ## (2,linkLab2)

    assert(g.nodes.size === 2)
    assert(g.edges.size === 2)
  }

  test("Successfully created dummy semantic model") {
    // create graph
    val g = dummyGraph
    val semModel = SemanticModel(g)

    assert(semModel.graph.nodes.size === 2)
    assert(semModel.graph.edges.size === 1)
  }

  test("Semantic model getNodes") {
    // create semantic model with immutable graph
    val semModel = SemanticModel(dummyGraph)
    val nodeList = semModel.getNodes.sortBy(x => x.id)

    assert(nodeList.size === 2)
    assert(nodeList === List(n1,n2))
  }

  test("Semantic model getLinks") {
    val g = scalax.collection.mutable.Graph(outer)
    val outer2 = n1 ~> n2 ## (2,linkLab2)
    g += outer2
    // create semantic model with mutable graph
    val semModel = SemanticModel(g)
    val linkList: List[SsdLink[semModel.NodeT]] = semModel.getLinks

    assert(linkList.size === 2)
    assert(linkList === List(outer, outer2))
  }

  test("Jsonify list of SSDNode") {
    val nodeList = List(n1,n2)
    val json = Extraction.decompose(nodeList)
    val nList = json.extract[List[SsdNode]]
    assert(nodeList === nList)
  }

  test("Jsonify semantic model") {
    val semModel = SemanticModel(dummyGraph)
    val json = Extraction.decompose(semModel)
    val semModel2 = json.extract[SemanticModel]
    assert(semModel === semModel2)
  }
  // test reading/writing the semantic model from json

  test("Read example semantic model from json") {
    Try {
      val stream = new FileInputStream(Paths.get(exampleSM).toFile)
      parse(stream).extract[SemanticModel]
    } match {
      case Success(semModel) =>
        assert(semModel.getNodes.size === 8)
        assert(semModel.getLinks.size === 7)
      case Failure(err) =>
        print(err.getMessage)
        fail(err.getMessage)
    }
  }

  test("Read empty semantic model from json") {
    Try {
      val stream = new FileInputStream(Paths.get(emptySM).toFile)
      parse(stream).extract[SemanticModel]
    } match {
      case Success(semModel) =>
        assert(semModel.getNodes.size === 0)
        assert(semModel.getLinks.size === 0)
      case Failure(err) =>
        println(err.getMessage)
        fail(err.getMessage)
    }
  }

  test("Adding isolate nodes to mutable scala graph") {
    val g: scalax.collection.mutable.Graph[SsdNode, SsdLink] = scalax.collection.mutable.Graph(n1)
    g += n2
    assert(g.nodes.size === 2)
  }
}


