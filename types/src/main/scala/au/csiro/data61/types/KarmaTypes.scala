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
package au.csiro.data61.types

import java.nio.file.Paths

import au.csiro.data61.types.SSDTypes.{AttrID, SsdID}
import au.csiro.data61.types.GraphTypes._
import com.typesafe.scalalogging.LazyLogging
import edu.isi.karma.rep.alignment.{ColumnNode, InternalNode, LabeledLink, Node}
import edu.isi.karma.rep.alignment.{SemanticType => KarmaSemanticType}
import edu.isi.karma.modeling.alignment.{SemanticModel => KarmaSSD}
import edu.isi.karma.modeling.alignment.learner.SortableSemanticModel
import org.jgrapht.graph.DirectedWeightedMultigraph

import scalax.collection.Graph
import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap
import scala.language.postfixOps

/**
  * Types to talk to Karma project
  */
object KarmaTypes {

}

/**
  * Enumeration of Node Type used in Karma
  */
sealed trait SSDNodeType { def str: String }

object SSDNodeType {

  case object NoneNode  extends SSDNodeType { val str = "None" }
  case object LiteralNode extends SSDNodeType { val str = "LiteralNode" }
  case object ColumnNode   extends SSDNodeType { val str = "ColumnNode" }
  case object InternalNode extends SSDNodeType { val str = "InternalNode" }

  val values = List(
    NoneNode,
    LiteralNode,
    ColumnNode,
    InternalNode
  )

  def lookup(str: String): Option[SSDNodeType] = {
    values.find(_.str == str)
  }
}

//LinkKeyInfo: None, UriOfInstance

/**
  * Enumeration of Link Type used in Karma
  */
sealed trait LinkType { def str: String }

object LinkType {

  case object NoneLink  extends LinkType { val str = "None" }
  case object CompactSubClassLink extends LinkType { val str = "CompactSubClassLink" }
  case object CompactObjectPropertyLink   extends LinkType { val str = "CompactObjectPropertyLink" }
  case object DataPropertyLink extends LinkType { val str = "DataPropertyLink" }
  case object ObjectPropertyLink extends LinkType { val str = "ObjectPropertyLink" }
  case object SubClassLink extends LinkType { val str = "SubClassLink" }
  case object ClassInstanceLink extends LinkType { val str = "ClassInstanceLink" }
  case object ColumnSubClassLink extends LinkType { val str = "ColumnSubClassLink" }
  case object DataPropertyOfColumnLink extends LinkType { val str = "DataPropertyOfColumnLink" }
  case object ObjectPropertySpecializationLink extends LinkType { val str = "ObjectPropertySpecializationLink" }

  val values = List(
    NoneLink,
    CompactSubClassLink,
    CompactObjectPropertyLink,
    DataPropertyLink,
    ObjectPropertyLink,
    SubClassLink,
    ClassInstanceLink,
    ColumnSubClassLink,
    DataPropertyOfColumnLink,
    ObjectPropertySpecializationLink
  )

  def lookup(str: String): Option[LinkType] = {
    values.find(_.str == str)
  }
}

/**
  * Enumeration of Link Status used in Karma
  */
sealed trait LinkStatus { def str: String }

object LinkStatus {

  case object ForcedByUser  extends LinkStatus { val str = "ForcedByUser" }
  case object PreferredByUI extends LinkStatus { val str = "PreferredByUI" }
  case object Normal extends LinkStatus { val str = "Normal" }

  val values = List(
    ForcedByUser,
    PreferredByUI,
    Normal
  )

  def lookup(str: String): Option[LinkStatus] = {
    values.find(_.str == str)
  }
}


/**
  * This class wraps around graphs returned by the Karma tool.
  * Such a graph is roughly equivalent to our semantic model.
  * It has a method to convert JGraphT to the scala graph object.
  *
  * @param graph Karma-like representation of the semantic model
  */
case class KarmaGraph(graph: DirectedWeightedMultigraph[Node,LabeledLink]) extends LazyLogging {

  /**
    * Assumed format: HN999
    * @param node Karma Node type
    * @return
    */
  private def toNodeID(node: Node): NodeID = {
    node match {
      case n: ColumnNode =>
        getColumnNodeId(n).filter(_.isDigit).toInt
      case _ =>
        node.getId.filter(_.isDigit).toInt
    }
  }

  /**
    * Helper function to split uri into namespace and value.
    * Namespace in uris is what goes before "#".
    * Value is what comes afterwards (e.g, name of class or name of property)
    * @param uri string
    * @return Tuple (namespace, value)
    */
  private def splitURI(uri: String): (String,String) = {
    uri.split("#") match {
      case Array(ns, value) => (ns + "#", value)
      case _ =>
        logger.info(s"Not proper uri $uri")
        ("", uri)
    }
  }

  /**
    * Helper function to create SSDLabel based on karma edge.
    * @param edge Karma representation of edge.
    * @return SSDLabel for the link
    */
  private def getLabel(edge: LabeledLink): SSDLabel = {
    val (ns, value) = splitURI(edge.getLabel.getUri)
    SSDLabel(value,
      edge.getType.toString,
      edge.getStatus.toString,
      ns) // TODO: should we check if namespace is proper? meaning it's present in prefixMap????
  }

  /**
    * Helper function to convert between node status for ColumnNodes in Karma and ours.
    * In Karma there is special semanticTypeStatus for ColumnNodes.
    * @param karmaStatus string which represents Karma status of node
    * @return
    */
  private def getColumnNodeStatus(karmaStatus: String): String = {
    karmaStatus.contains("User") match {
      case true => "ForcedByUser" // in Karma there are options: UserAssigned...
      case false => karmaStatus   // we don't do anything
    }
  }

  /**
    * Helper function to get label of the Karma ColumnNode.
    * @param n ColumnNode
    * @return String
    */
  private def getColumnNodeLabel(n: ColumnNode): String = {
    // get the label of this node... tricky for ColumnNode
    // NOTE: the assumption is that the UserSemanticTypes of ColumnNode contain the mapping of the source column
    // into the semantic model and that there is only one semantic type there.
    // --- I haven't seen Karma produce any source description with more than one user semantic type for ColumnNode...
    val label: String = n.getUserSemanticTypes.asScala.map {
      semtype  => {
        val firstPart = splitURI(semtype.getDomain.getUri)._2 // this is the name of the class
        val secondPart = splitURI(semtype.getType.getUri)._2 // this is the name of the property
        s"$firstPart.$secondPart"
      }
    }.toList match {
      case List(s: String) => s
      case List() =>
        logger.warn("UserSemanticType is not properly set in Karma.")
        "" // basically, the semantic type is not properly set in Karma
      case _ =>
        logger.error("We do not support multiple user semantic types.")
        throw new Exception("We do not support multiple user semantic types.")
    }
    label
  }

  /**
    * Helper function to get id of the Karma ColumnNode.
    * We cannot just take Karma id, we need to look at the one specified in the UserSemanticTypes.
    * @param n ColumnNode
    * @return String
    */
  private def getColumnNodeId(n: ColumnNode): String = {
    // --- I haven't seen Karma produce any source description with more than one user semantic type for ColumnNode...
    val colId: String = n.getUserSemanticTypes.asScala.map(_.getHNodeId).toList match {
      case List(s: String) =>
        logger.debug(s"=> UserSemanticType: ${s}")
        s
      case List() =>
        logger.warn("UserSemanticType is not available hence taking Karma hNodeId.")
        n.getHNodeId // basically, returning Karma node id
      case _ =>
        logger.error("We do not support multiple user semantic types.")
        throw new Exception("We do not support multiple user semantic types.")
    }
    colId
  }

  /**
    * Helper function to create SSDLabel based on karma node.
    * @param node Karma representation of node.
    * @return SSDLabel for the link
    */
  private def getLabel(node: Node): SSDLabel = {
    val nodeStatus: String = node.isForced match {
      case true => "ForcedByUser"
      case _ => "Normal"}
    node match {
      case n: ColumnNode =>
        SSDLabel(getColumnNodeLabel(n),
          "DataNode",
          getColumnNodeStatus(n.getSemanticTypeStatus.toString), // ColumnNode has special status field
          "") // DataNodes will have empty prefix!
      case n: InternalNode =>
        val (ns, value) = splitURI(node.getLabel.getUri)
        // NOTE: id of InternalNode is not just the uri of the corresponding class node in the ontology.
        // additionally, there is a counter associated to the class uri.
        // we have to consider that when converting our representation to Karma.
        SSDLabel(value,
          "ClassNode", // TODO: can InternalNode be anything else than ClassNode?
          nodeStatus, ns)
      case _ =>
        if (node.getLabel.getUri == "") {
          SSDLabel(node.getId,
            node.getType.toString, // NOTE: there are still None and LiteralNode in Karma
            "Poor", "")
        }
        else {
          val (ns, value) = splitURI(node.getLabel.getUri)
          SSDLabel(value,
            node.getType.toString, // NOTE: there are still None and LiteralNode in Karma
            "Poor", ns)
        }
    }
  }

  /**
    * Helper function to get mapping from Karma NodeId (String) to SSDNode.
    * @return Map from string Karma node id to SSDNode
    */
  private def karmaNodeIdMap: Map[String,SSDNode] = {
    graph.vertexSet.asScala
      .zipWithIndex
      .map {
        case (node, nodeID) =>
          node.getId -> SSDNode(nodeID,getLabel(node))
      } toMap
  }

  /**
    * Get mapping of attributes to nodes in the semantic model.
    * We sort the Map by keys.
    */
  def columnNodeMappings: Map[AttrID,NodeID] = {
    logger.debug("Calculating columnNodeMappings")
    ListMap(graph.vertexSet.asScala
      .filter(_.isInstanceOf[ColumnNode])
      .map {
        node => {
          logger.debug(s"mapping ${node.getId}")
          toNodeID(node) -> karmaNodeIdMap(node.getId).id
        }
    }.toSeq.sortBy(_._1):_*)
  }

  /**
    * Method to convert Karma representation of the semantic model to our data structure.
    * @return SemanticModel
    */
  def toSemanticModel: SemanticModel = {
    logger.info("Converting Karma Graph to the Semantic Model...")
    logger.debug(s"##################karmaNodeId: ${karmaNodeIdMap}")
    // converting links
    val ssdLinks: List[SSDLink[SSDNode]] = graph.edgeSet.asScala
      .zipWithIndex
      .map {
        case (edge, linkID) =>
          logger.debug(s"*******Converting link $linkID")
          logger.debug(s"*******sourceNode ${edge.getSource.getId}")
          val sourceNode: SSDNode = karmaNodeIdMap(edge.getSource.getId)
          logger.debug(s"*******targetNode ${edge.getTarget.getId}")
          val targetNode: SSDNode = karmaNodeIdMap(edge.getTarget.getId)

          SSDLink(sourceNode,targetNode,linkID,getLabel(edge))
      } toList

    logger.debug(s"##################Links add")

    val sm: Graph[SSDNode,SSDLink] = Graph()
    SemanticModel(sm ++ ssdLinks)
  }

}

// Karma semantic model === SemanticSourceDescription
/**
  * This class wraps around semantic source descriptions returned by Karma tool.
  * It has a method to convert to our SSD representation.
  *
  * @param karmaModel Karma-like representation of the semantic source description
  */
case class KarmaSemanticModel(karmaModel: KarmaSSD) extends LazyLogging {

  /**
    * karmaModel.graph corresponds to our semantic model
    */
  def karmaSM: KarmaGraph = KarmaGraph(karmaModel.getGraph)

  /**
    * Karma uses strings as ids.
    * Assumed Karma format: HN999
    * This method takes only numbers from the string.
    * Here we assume that when our SSD gets first converted to Karma type, we create node ids by adding 'HN' to the NodeID.
    * That's why when converting back from Karma, we should get correct ids.
    * @param node Karma ColumnNode type
    * @return
    */
  private def toID(node: ColumnNode): NodeID = node.getId.filter(_.isDigit).toInt

  /**
    * Helper function to convert Karma:SemanticModel.sourceColumns to SSDColumn
    * @return
    */
  protected def ssdColumns: List[SSDColumn] = {
    karmaModel.getSourceColumns.asScala // these should be attributes in our SSD, they are not source columns!
      .map { sourceCol => SSDColumn(toID(sourceCol), sourceCol.getColumnName) } toList
  }

  /**
    * Helper function to convert Karma:SemanticModel.sourceColumns to SSDAttribute
    * @param tableName Name of the table to be used in the default sql identity map statement.
    * @return
    */
  protected def ssdAttributes(tableName: String = ""): List[SSDAttribute] = {
    ssdColumns
      .map( sourceCol => SSDAttribute(sourceCol, sourceCol.id, tableName))
  }

  /**
    * This method converts the Karma-like SSD to our SemanticSourceDesc.
    * NOTE: this method should not be really used!
    * NOTE: Instead of converting KarmaSemanticModel we should rather update existing SSD with info from KarmaSM
    * @param newID id of the SSD. Karma tool has weird ids for its resources.
    * @param ssdVersion String which corresponds to the version of SSD
    * @param ontologies List of Strings which correspond to paths where ontologies are lying
    * @param tableName Name of the table to be used in the default sql identity map statement.
    * @return SSD
    */
  def toSSD(newID: SsdID,
            ssdVersion: String,
            ontologies: List[String],
            tableName: String = ""): SemanticSourceDesc  = {
    logger.info("Converting Karma Semantic Model to SSD...")
    // karmaModel.sourceColumns: List[ColumnNode] --> List[SSDAttribute], and also List[SSDColumn]
    // ColumnNodes in karmaModel.graph correspond to our mappings
    // get all ontologies from karma/preloaded-ontologies directory

    SemanticSourceDesc(version = ssdVersion,
      name = karmaModel.getName,
      id = newID,
      columns = ssdColumns,
      attributes = ssdAttributes(tableName),
      ontology = ontologies, // here we create instance of KarmaParams...
      semanticModel = Some(karmaSM.toSemanticModel),
      mappings = Some(SSDMapping(karmaSM.columnNodeMappings)))
  }

}

/**
  * This class wraps around semantic source descriptions returned by Karma tool.
  * It has a method to convert to our SSD representation.
  *
  * @param karmaModel Karma-like representation of the semantic source description
  */
case class KarmaSortableSemanticModel(karmaModel: SortableSemanticModel){

  /**
    * karmaModel.graph corresponds to our semantic model
    */
  def karmaSM: KarmaGraph = KarmaGraph(karmaModel.getGraph)

  /**
    * Returns the score for the mapping.
    * This is a weighted average of node coherence, confidence and size reduction.
    */
  def mappingScore: Double = karmaModel.getScore

  /**
    * Returns the score for the node coherence.
    */
  def nodeCoherence: Double = karmaModel.getSteinerNodes.getCoherence.getCoherenceValue

  /**
    * Returns the score for the node confidence.
    */
  def nodeConfidence: Double = karmaModel.getConfidenceScore

  /**
    * Returns the size reduction score.
    * karmaModel.getSteinerNodes.getSizeReduction is a private method in the original Karma source code -- I've changed it!!!
    */
  def sizeReduction: Double = karmaModel.getSteinerNodes.getSizeReduction

  /**
    * Returns the score for the link coherence.
    */
  def linkCoherence: Double = karmaModel.getLinkCoherence.getCoherenceValue

  /**
    * Returns the score for the cost of the semantic model when solving minimum cost Steiner Tree problem.
    */
  def cost: Double = karmaModel.getCost

  /**
    * This method converts the Karma-like SSD to our SemanticSourceDesc.
    * @param newID id of the SSD. Karma tool has weird ids for its resources.
    * @param ssdVersion String which corresponds to the version of SSD
    * @param ontologies List of Strings which correspond to paths where ontologies are lying
    * @param tableName Name of the table to be used in the default sql identity map statement.
    * @return SSD
    */
  def toSSD(newID: SsdID,
            ssdVersion: String,
            ontologies: List[String],
            tableName: String = ""): SemanticSourceDesc  = {
    // TODO: implement
    KarmaSemanticModel(karmaModel.getBaseModel).toSSD(newID, ssdVersion, ontologies, tableName)
  }

}