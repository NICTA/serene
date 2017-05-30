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
package au.csiro.data61.modeler.karma

import java.nio.file.Paths

import au.csiro.data61.types.Ssd
import au.csiro.data61.types.Exceptions._
import com.typesafe.scalalogging.LazyLogging
import edu.isi.karma.modeling.alignment.Alignment
import edu.isi.karma.modeling.alignment.learner.{ModelLearningGraph, ModelLearningGraphType, PatternWeightSystem}
import edu.isi.karma.rep.alignment.InternalNode

import scala.util.{Try, Failure, Success}

/**
  * As input we give a list of known ssd.
  * Karma computes the alignment graph.
 *
  * @param karmaWrapper Object which holds initialized Karma parameters
  */

case class KarmaBuildAlignmentGraph(karmaWrapper: KarmaParams) extends LazyLogging {

  /**
    * needed for alignment construction in Karma
    */
  protected var modelLearningGraph = ModelLearningGraph
    .getEmptyInstance(karmaWrapper.karmaWorkspace.getOntologyManager, ModelLearningGraphType.Compact)

  /**
    * stores the alignment graph as constructed by Karma
    */
  var alignment: Alignment = {
    val alg = new Alignment(karmaWrapper.karmaWorkspace.getOntologyManager)
    alg.setGraph(modelLearningGraph.getGraphBuilderClone.getGraph)
    alg
  }

  /**
    * Read in most recent Karma stuff from the disk.
    * FIXME: how to properly keep track of the state of Karma stuff???
    */
  def karmaInitialize(): Unit = {
    //karmaWrapper = KarmaParams()
    // we are re-reading the file graph.json if it exists when we use ModelLearningGraph.getInstance()
    // getEmptyInstance should reset it
    modelLearningGraph = ModelLearningGraph
      .getEmptyInstance(karmaWrapper.karmaWorkspace.getOntologyManager, ModelLearningGraphType.Compact)
    logger.debug(s"modelLearningGraph: ${modelLearningGraph.getGraphBuilder.getGraph.vertexSet.size} nodes, " +
      s"${modelLearningGraph.getGraphBuilder.getGraph.edgeSet.size} links.")
    alignment = new Alignment(karmaWrapper.karmaWorkspace.getOntologyManager)
    alignment.setGraph(modelLearningGraph.getGraphBuilderClone.getGraph)

  }

  /**
    * export the alignment graph into karma folders and change the modified time
    */
  private def exportAlignmentGraph(): Unit = {
    logger.debug(s"Export of the alignment graph")
    modelLearningGraph.exportJson()
    modelLearningGraph.exportGraphviz()
    // NOTE: mLearningGraph.lastUpdateTime is private --- I've added one more method to Karma to set it!
    modelLearningGraph.setLastUpdateTime(java.lang.System.currentTimeMillis)
  }

  private def addOntologyPaths(addedNodes: java.util.Set[InternalNode]): Unit = {
    if(karmaWrapper.karmaModelingConfiguration.getAddOntologyPaths) {
      // ontology inference
      logger.debug("Adding node closure and links from the ontology...")
      Try {
        modelLearningGraph.updateGraphUsingOntology(addedNodes)
      } match {
        case Success(s) =>
          logger.debug("Ontology paths added successfully!")
        case Failure(err) =>
          logger.warn(s"Ontology paths not added: ${err.getMessage}")
      }
    }
  }

  /**
    * This method constructs the initial alignment graph based on preloaded onotologies + known SSDs in SSDStorage.
    * @param knownSsds List of known semantic source descriptions
    * @return alignment
    */
  def constructInitialAlignment(knownSsds: List[Ssd]) : Alignment = {

    logger.info("Constructing the alignment graph...")
    karmaInitialize()

    // if karmaModelingConfiguration.getKnownModelsAlignment is set to true,
    // inside the Karma constructor method alignment graph will be constructed based on preloaded ontologies
    // plus known semantic models in folder with Karma JSON model

    // we need to add our ssd models from SSDStorage
    if(karmaWrapper.karmaModelingConfiguration.getKnownModelsAlignment) {

      logger.info(s"Adding known SSDs to the alignment graph: ${knownSsds.size} to go...")

      // adding of semantic models to the alignment graph is handled within ModelLearningGraph class in Karma.
      // there are two types of graphs: compact and sparse... compact is used mainly in Karma code.

      // we keep track of nodes added to the graph since it's needed later on for ontology inference
      var addedNodes: java.util.Set[InternalNode] = new java.util.HashSet[InternalNode]

      knownSsds
        .flatMap { sm =>
          logger.debug(s" converting ssd (${sm.id}, ${sm.name}) to karma model")
          sm.toKarmaSemanticModel(alignment.getGraphBuilder.getOntologyManager)
        }
        .foreach {
          karmaModel =>
            // here we add model to the alignment graph
            val temp: java.util.Set[InternalNode] =
              modelLearningGraph.addModel(
                karmaModel.karmaModel,
                PatternWeightSystem.JWSPaperFormula // TODO: understand the difference between formulas
              )
            if (temp != null) {addedNodes.addAll(temp)}
            logger.debug(s" karma model added")
        }
      exportAlignmentGraph()
      // ontology inference
      addOntologyPaths(addedNodes)

      exportAlignmentGraph()
      alignment.setGraph(modelLearningGraph.getGraphBuilderClone.getGraph)
    }


    alignment
  }

  /**
    * Add a new semantic source description to the alignment graph.
    * If the SSD is incomplete, a Modeler Exception will be raised.
    * @param ssd New Semantic Source Description to be added to the alignment graph.
    * @return Alignment
    */
  def add(ssd: Ssd): Alignment = {

    logger.info("Adding an SSD to the alignment graph...")

    if (!ssd.isComplete) {
      logger.error("ModelerException: Cannot add an incomplete SSD to the Alignment Graph!")
      throw ModelerException("Cannot add an incomplete SSD to the Alignment Graph!")
    }

    ssd.toKarmaSemanticModel(karmaWrapper.karmaWorkspace.getOntologyManager) match {
      case Some(karmaModel) =>
        // here we add model to the alignment graph, update using ontologies and export to JSON
        modelLearningGraph.addModelAndUpdateAndExport (
          karmaModel.karmaModel,
          PatternWeightSystem.JWSPaperFormula // TODO: understand the difference between formulas
       )
      case None =>
        logger.warn("SSD not added to the alignment graph since convertion to Karma style failed!")
    }
    // NOTE: mLearningGraph.lastUpdateTime is private --- I've added one more method to Karma to set it!
    modelLearningGraph.setLastUpdateTime(java.lang.System.currentTimeMillis)

    alignment.setGraph(modelLearningGraph.getGraphBuilderClone.getGraph)
    alignment
  }

  /**
    * Realign the graph.
    * This needs to be done if there are changes to ontologies, changes to the existing known SSDs or SSD is deleted.
    * NOTE: It's better to avoid this!
 *
    * @return Alignment
    */
  def realign(knownSsds: List[Ssd]): Alignment = {
    logger.info("Re-constructing the alignment graph...")

    // create empty alignment graph
    modelLearningGraph = ModelLearningGraph
      .getEmptyInstance(karmaWrapper.karmaWorkspace.getOntologyManager, ModelLearningGraphType.Compact)

    // adding of semantic models to the alignment graph is handled within ModelLearningGraph class in Karma.
    // there are two types of graphs: compact and sparse... compact is used mainly in Karma code.
    knownSsds
      .flatMap(_.toKarmaSemanticModel(karmaWrapper.karmaWorkspace.getOntologyManager))
      .foreach {
        karmaModel =>
          // here we add model to the alignment graph and update using pre-loaded ontologies
          modelLearningGraph.addModelAndUpdate(
            karmaModel.karmaModel,
            PatternWeightSystem.JWSPaperFormula // TODO: understand the difference between formulas
          )
      }
    // export the alignment graph into karma folders
    modelLearningGraph.exportJson()
    modelLearningGraph.exportGraphviz()
    // NOTE: mLearningGraph.lastUpdateTime is private --- I've added one more method to Karma to set it!
    modelLearningGraph.setLastUpdateTime(java.lang.System.currentTimeMillis)
    alignment.setGraph(modelLearningGraph.getGraphBuilderClone.getGraph)
    alignment
  }
}
