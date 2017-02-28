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

import au.csiro.data61.types._
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.scalatest.junit.JUnitRunner

import language.postfixOps
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import com.typesafe.scalalogging.LazyLogging
import org.jgrapht.graph.DirectedWeightedMultigraph
import edu.isi.karma.rep.alignment.{DefaultLink, Node}
import edu.isi.karma.modeling.alignment.GraphUtil
import au.csiro.data61.modeler.karma.{KarmaBuildAlignmentGraph, KarmaParams}
import au.csiro.data61.types.Exceptions.ModelerException


/**
  * Tests for building the alignment graph
  */

@RunWith(classOf[JUnitRunner])
class AlignmentGraphSpec extends FunSuite with ModelerJsonFormats with BeforeAndAfterEach with LazyLogging {

  val ssdDir = getClass.getResource("/ssd").getPath
  val karmaDir = getClass.getResource("/karma").getPath
  val alignmentDir = Paths.get("/tmp/test-ssd", "alignment") toString
  val exampleSSD: String = Paths.get(ssdDir,"businessInfo.ssd") toString
  val citiesSSD: String = Paths.get(ssdDir,"getCities.ssd") toString
  val emptySSD: String = Paths.get(ssdDir,"empty_model.ssd") toString
  val exampleKarmaSSD: String = Paths.get(karmaDir,"businessInfo.csv.model.json") toString
  val exampleSM: String = Paths.get(ssdDir,"semantic_model_example.json") toString
  val businessAlign: String = Paths.get(karmaDir,"align_business.json") toString
  val businessCitiesAlign: String = Paths.get(karmaDir,"align_business_cities.json") toString
  val exampleOntol: String = Paths.get(ssdDir,"dataintegration_report_ontology.owl") toString

  var knownSSDs: List[SemanticSourceDesc] = List()
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
      parse(stream).extract[SemanticSourceDesc]
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

  test("Constructing initial alignment graph using Karma") {
    val karmaTrain = KarmaBuildAlignmentGraph(karmaWrapper)

    // our alignment
    val alignment = karmaTrain.constructInitialAlignment(knownSSDs)

    // this is the output from running Web-Karma
    val graph: DirectedWeightedMultigraph[Node, DefaultLink] =
      GraphUtil.importJson(businessAlign)

    val resultLinks = alignment.getGraph
      .edgeSet.asScala.map {
        e => (e.getSource.getLabel.getUri, e.getTarget.getLabel.getUri, e.getUri, e.getWeight, e.getType)
    }.toList.sorted

    val karmaLinks = graph.edgeSet.asScala.map {
      e => (e.getSource.getLabel.getUri, e.getTarget.getLabel.getUri, e.getUri, e.getWeight, e.getType)
    }.toList.sorted

    assert(resultLinks === karmaLinks)
  }

  test("Duplicating SSD to the alignment graph") {
    val karmaTrain = KarmaBuildAlignmentGraph(karmaWrapper)
    // our alignment
    var alignment = karmaTrain.alignment
    assert(alignment.getGraph.vertexSet.size === 0)
    assert(alignment.getGraph.edgeSet.size === 0)

    alignment = karmaTrain.constructInitialAlignment(knownSSDs)
    assert(alignment.getGraph.vertexSet.size === 8)
    assert(alignment.getGraph.edgeSet.size === 7)


    // TODO: to forbid addition of the SSD more than once to the alignment
    // this should be done once Alignment Storage is set up!
    val newSSD: SemanticSourceDesc = knownSSDs.headOption match {
      case Some(ssd) => ssd.copy(id = Some(1))
      case None =>
        fail("SSD 0 is missing!")
    }

    // we add the same semantic model --> that should influence only the weights of object property links!
    alignment = karmaTrain.add(newSSD)
    assert(alignment.getGraph.vertexSet.size === 8)
    assert(alignment.getGraph.edgeSet.size === 7)

    // this is the output from running Web-Karma
    val graph: DirectedWeightedMultigraph[Node, DefaultLink] =
    GraphUtil.importJson(businessAlign)

    val resultLinks = alignment.getGraph
      .edgeSet.asScala.map {
      e => (e.getSource.getLabel.getUri, e.getTarget.getLabel.getUri, e.getUri, e.getType)
    }.toList.sorted

    val weights = alignment.getGraph
      .edgeSet.asScala.toList.map(_.getWeight).sorted

    val karmaLinks = graph.edgeSet.asScala.map {
      e => (e.getSource.getLabel.getUri, e.getTarget.getLabel.getUri, e.getUri, e.getType)
    }.toList.sorted

    assert(resultLinks === karmaLinks)
    // TODO: how to check if the weights are correct???
    assert(weights === List(0.33333333333333337, 0.33333333333333337, 0.33333333333333337, 1.0, 1.0, 1.0, 1.0))

  }

  test("Adding cities SSD to the alignment graph") {
    val karmaTrain = KarmaBuildAlignmentGraph(karmaWrapper)
    // our alignment
    var alignment = karmaTrain.alignment
    assert(alignment.getGraph.vertexSet.size === 0)
    assert(alignment.getGraph.edgeSet.size === 0)

    alignment = karmaTrain.constructInitialAlignment(knownSSDs)
    assert(alignment.getGraph.vertexSet.size === 8)
    assert(alignment.getGraph.edgeSet.size === 7)

    val newSSD = Try {
      val stream = new FileInputStream(Paths.get(citiesSSD).toFile)
      parse(stream).extract[SemanticSourceDesc]
    } match {
      case Success(ssd) =>
        ssd
      case Failure(err) =>
        fail(err.getMessage)
    }

    // we add the same semantic model --> that should influence only the weights of object property links!
    alignment = karmaTrain.add(newSSD)
    assert(alignment.getGraph.vertexSet.size === 8)
    assert(alignment.getGraph.edgeSet.size === 7)

    // this is the output from running Web-Karma
    val graph: DirectedWeightedMultigraph[Node, DefaultLink] =
    GraphUtil.importJson(businessCitiesAlign)

    val resultLinks = alignment.getGraph
      .edgeSet.asScala.map {
      e => (e.getSource.getLabel.getUri, e.getUri, e.getType, e.getWeight)
    }.toList.sorted

    val weights = alignment.getGraph
      .edgeSet.asScala.toList.map(_.getWeight).sorted

    // resultLinks and karmaLinks have different ColumnNodes names, so I exclude them
    val karmaLinks = graph.edgeSet.asScala.map {
      e => (e.getSource.getLabel.getUri, e.getUri, e.getType, e.getWeight)
    }.toList.sorted

    // weights of the links are also checked
    assert(resultLinks === karmaLinks)

  }


  test("Adding inconsistent SSD to the alignment graph should fail") {
    val karmaTrain = KarmaBuildAlignmentGraph(karmaWrapper)
    // our alignment
    var alignment = karmaTrain.alignment

    val newSSD = Try {
      val stream = new FileInputStream(Paths.get(emptySSD).toFile)
      parse(stream).extract[SemanticSourceDesc]
    } match {
      case Success(ssd) =>
        ssd
      case Failure(err) =>
        fail(err.getMessage)
    }

    Try {
      karmaTrain.add(newSSD)
    } match {
      case Success(align) => fail("Inconsistent SSD should not be added to the alignment graph!!!")
      case Failure(err) => succeed
    }
  }

  test("Re-aligning the graph") {
    val karmaTrain = KarmaBuildAlignmentGraph(karmaWrapper)
    // our alignment
    var alignment = karmaTrain.alignment

    // removing all SSDs
    knownSSDs = List()
    alignment = karmaTrain.realign(knownSSDs)

    val resultLinks = alignment.getGraphBuilder.getGraph
      .edgeSet.asScala.map {
      e => (e.getSource.getLabel.getUri, e.getTarget.getLabel.getUri, e.getUri, e.getWeight, e.getType)
    }.toList.sorted

    assert(resultLinks.size === 0)

  }

}
