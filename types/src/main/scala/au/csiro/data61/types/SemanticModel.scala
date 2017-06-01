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

import au.csiro.data61.types.Exceptions.TypeException
import au.csiro.data61.types.GraphTypes._
import com.typesafe.scalalogging.LazyLogging
import edu.isi.karma.modeling.ontology.OntologyManager
import edu.isi.karma.rep.alignment.SemanticType.{Origin => KarmaOrigin}
import edu.isi.karma.rep.alignment.{LabeledLink, SubClassLink, ClassInstanceLink, DataPropertyLink, ObjectPropertyLink, CompactSubClassLink}
import edu.isi.karma.rep.alignment.{ColumnNode, InternalNode, LiteralNode, Node, ObjectPropertyType}
import edu.isi.karma.rep.alignment.{Label => KarmaLabel, LinkStatus => AlignLinkStatus, SemanticType => KarmaSemanticType}
import org.jgrapht.graph.DirectedWeightedMultigraph
import org.json4s._

import scala.language.{implicitConversions, postfixOps}
import scala.util.{Failure, Success, Try}
import scalax.collection.Graph
import scalax.collection.GraphEdge._
import scalax.collection.GraphPredef._

/**
  * Types used for nodes and links
  */
object GraphTypes {
  type NodeID = Int
  type LinkID = Int
}


/**
  * Case class to represent both node and link labels
  * We should separate labels for links and nodes, since labelTypes should be specific enums...
 *
  * @param label node or link label
  * @param labelType node or link type
  * @param status node or link status
  * @param prefix this is actually namespace, default to the one indicated in Config
  */
case class SsdLabel(label: String,
                    labelType: String,
                    status: String = "ForcedByUser",
                    prefix: String = TypeConfig.DefaultNamespace) extends Ordered[SsdLabel]{
  override def toString: String = s"($prefix, $label, $labelType, $status)"

  def getURI: String = s"$prefix$label"

  def compare(that: SsdLabel): Int = this.toString.compare(that.toString)
}

/**
  * Case class which specifies node type in the semantic model.
  * We might have nodes which have the same ssdLabels, but different ids.
 *
  * @param id id of the node
  * @param ssdLabel SSDLabel of the node
  */
case class SsdNode(id: NodeID,
                   ssdLabel: SsdLabel)
{
  override def toString: String = s"$id:$ssdLabel"
}

/**
  * custom JSON serializer for the SSDNode
  */
case object SsdNodeSerializer extends CustomSerializer[SsdNode](
  format => (
    {
      case jv: JValue =>
        implicit val formats = DefaultFormats
        val nId: NodeID = HelperJSON.parseOption[NodeID]("id",jv) match {
          case Success(Some(v)) =>
            v
          case Success(None) =>
            throw TypeException("Failed to parse SSDNode: absent id!")
          case Failure(err) =>
            throw TypeException(s"Failed to parse SSDNode: ${err.getMessage}")
        }
        val nStatus: String = HelperJSON.parseOption[String]("status",jv) match {
          case Success(Some(v)) =>
            v
          case Success(None) =>
            "ForcedByUser" // default value
          case Failure(err) =>
            throw TypeException(s"Failed to parse SSDNode: ${err.getMessage}")
        }
        val nLab: String = HelperJSON.parseOption[String]("label",jv) match {
          case Success(Some(v)) =>
            v
          case Success(None) =>
            throw TypeException("Failed to parse SSDNode: absent label!")
          case Failure(err) =>
            throw TypeException(s"Failed to parse SSDNode: ${err.getMessage}")
        }
        val nType: String = HelperJSON.parseOption[String]("type",jv) match {
          case Success(Some(v)) =>
            v
          case Success(None) =>
            throw TypeException("Failed to parse SSDNode: absent type!")
          case Failure(err) =>
            throw TypeException(s"Failed to parse SSDNode: ${err.getMessage}")
        }
        val nPrefix: String = HelperJSON.parseOption[String]("prefix",jv) match {
          case Success(Some(v)) =>
            v
          case Success(None) =>
            nType match {
              case "ClassNode" =>
                TypeConfig.DefaultNamespace // default namespace is used only for ClassNodes
              case _ =>
                ""                          // for all other node types we use an empty string
            }
          case Failure(err) => throw TypeException(s"Failed to parse SSDNode: ${err.getMessage}")
        }
        SsdNode(nId, SsdLabel(nLab,nType,nStatus, nPrefix))
    }, {
    case node: SsdNode =>
      // really annoying: I can't use ~ since it's in scala graph library
      JObject(JField("id", JInt(node.id)) ::
          JField("label", JString(node.ssdLabel.label)) ::
          JField("type", JString(node.ssdLabel.labelType)) ::
          JField("status", JString(node.ssdLabel.status)) ::
          JField("prefix", JString(node.ssdLabel.prefix)) :: Nil)
  })
)

/**
  * Case class which specifies link type in the semantic model.
  * It is a custom edge specified on the base of a directed edge from scala-graph library.
  *
  * @param fromNode id of the source node
  * @param toNode id of the target node
  * @param id id of the link
  * @param ssdLabel SSDLabel of the link
  */
case class SsdLink[SsdNode](fromNode: SsdNode,
                            toNode: SsdNode,
                            id: LinkID,
                            ssdLabel: SsdLabel)
  extends DiEdge[SsdNode](NodeProduct(fromNode,toNode))
  with ExtendedKey[SsdNode]
  with EdgeCopy[SsdLink]
  with OuterEdge[SsdNode,SsdLink]
{
  private def this(nodes: Product, id: LinkID, ssdLabel: SsdLabel) {
    this(nodes.productElement(0).asInstanceOf[SsdNode],
      nodes.productElement(1).asInstanceOf[SsdNode], id, ssdLabel)
  }
  def keyAttributes: Seq[LinkID] = Seq(id)

  override def copy[NN](newNodes: Product): SsdLink[NN] = new SsdLink[NN](newNodes, id, ssdLabel)

  override protected def attributesToString = s" ## ($id,$ssdLabel)"
}

// specifying a shortcut for edge creation
object SsdLink {
  implicit final class ImplicitEdge[A <: SsdNode](val e: DiEdge[A]) extends AnyVal {
    //    def ## (id: LinkID) = new SSDLink[A](e.source, e.target, id)
    // this was taken from the guidelines, but it doesn't work for some unknown reasons....
    def ##(id: LinkID, ssdLabel: SsdLabel): SsdLink[A] = new SsdLink[A](e.nodes, id, ssdLabel)
    // we now create a new link by typing:
    // SSDNode(1, ssdLabel) ~> SSDNode(2,ssdLabel) ## (1, ssdLabel)
  }
}

/**
  * Helper class for json serialization
 *
  * @param id Link id
  * @param source Node id of the source
  * @param target Node id of the target
  * @param label String for link label
  * @param lType String for link type
  * @param status String for link status
  * @param prefix Namespace of the link label, default namespace is set in Config.
  */
case class HelperLink(id: LinkID,
                      source: NodeID,
                      target: NodeID,
                      label: String,
                      lType: String,
                      status: String = "ForcedByUser",
                      prefix: String = TypeConfig.DefaultNamespace)
//custom JSON serializer for the HelperLink
case object HelperLinkSerializer extends CustomSerializer[HelperLink](
  format => (
    {
      case jv: JValue =>
        implicit val formats = DefaultFormats
        val id: LinkID = HelperJSON.parseOption[LinkID]("id",jv) match {
          case Success(Some(v)) => v
          case Success(None) => throw TypeException("Failed to parse link: absent id!")
          case Failure(err) => throw TypeException(s"Failed to parse link. ${err.getMessage}")
        }
        val source: NodeID = HelperJSON.parseOption[NodeID]("source",jv) match {
          case Success(Some(v)) => v
          case Success(None) => throw TypeException("Failed to parse link: absent source id!")
          case Failure(err) => throw TypeException(s"Failed to parse link. ${err.getMessage}")
        }
        val target: NodeID = HelperJSON.parseOption[NodeID]("target",jv) match {
          case Success(Some(v)) => v
          case Success(None) => throw TypeException("Failed to parse link: absent target id!")
          case Failure(err) => throw TypeException(s"Failed to parse link. ${err.getMessage}")
        }
        val status: String = HelperJSON.parseOption[String]("status",jv) match {
          case Success(Some(v)) => v
          case Success(None) => "ForcedByUser" // default value
          case Failure(err) => throw TypeException(s"Failed to parse link. ${err.getMessage}")
        }
        val label: String = HelperJSON.parseOption[String]("label",jv) match {
          case Success(Some(v)) => v
          case Success(None) => throw TypeException("Failed to parse link: absent label!")
          case Failure(err) => throw TypeException(s"Failed to parse link. ${err.getMessage}")
        }
        val lType = HelperJSON.parseOption[String]("type",jv) match {
          case Success(Some(v)) => v
          case Success(None) => throw TypeException("Failed to parse link: absent type!")
          case Failure(err) => throw TypeException(s"Failed to parse link. ${err.getMessage}")
        }
        val lPrefix: String = HelperJSON.parseOption[String]("prefix",jv) match {
          case Success(Some(v)) => v
          case Success(None) => TypeConfig.DefaultNamespace // default value
          case Failure(err) => throw TypeException(s"Failed to parse link: ${err.getMessage}")
        }
        HelperLink(id, source, target, label, lType, status, lPrefix)
    }, {
    case link: HelperLink =>
      // really annoying: I can't use ~ since it's in scala graph library
      JObject(JField("id", JInt(link.id)) ::
        JField("source", JInt(link.source)) ::
        JField("target",JInt(link.target)) ::
        JField("label",JString(link.label)) ::
        JField("type",JString(link.lType)) ::
        JField("status",JString(link.status)) ::
        JField("prefix",JString(link.prefix)) :: Nil)
  })
)


/**
  * Karma-tool uses JGraphT library to represent graphs.
  * Moving to ScalaGraph library... we need converters from JGraphT to Scala Graph and vice versa.
  * Also Json serializer is needed to write ssd files.
 *
  * @param graph graph object from Scala Graph library
  */
case class SemanticModel(graph: Graph[SsdNode, SsdLink]) extends LazyLogging {
  type NodeT = graph.NodeT // SsdNode
  type LinkT = graph.EdgeT // SsdLink[SsdNode]

  /**
    * Method to get all nodes from the graph of the semantic model.
    * This method is needed for Json serialization.
 *
    * @return List[SsdNode]
    */
  def getNodes: List[SsdNode] = graph.nodes.map(n => n.value) toList

  /**
    * Method to get the sorted list of node labels.
    * This method can be used to compare nodes of two different semantic models.
 *
    * @return List[SsdNode]
    */
  def getNodeLabels: List[SsdLabel] = graph.nodes
    .map(_.ssdLabel).toList.sorted

  /**
    * Method to get all links from the graph of the semantic model.
 *
    * @return List of SsdLink[NodeT] where NodeT should be equal to SSDNode
    */
  def getLinks: List[SsdLink[NodeT]] = graph.edges
    .map(_.edge.asInstanceOf[SsdLink[NodeT]]) toList

  /**
    * Method to get the list of lexicographically ordered link labels:
    * (sourceLabel, linkLabel, targetLabel).
    * This method can be used to compare links of two different semantic models.
 *
    * @return List of triplets, each element corresponding to SSDLabel
    */
  def getLinkLabels: List[(SsdLabel, SsdLabel,SsdLabel)] = graph.edges
    .map(e => (e.fromNode.ssdLabel, e.ssdLabel, e.toNode.ssdLabel)).toList
    .sortBy(e => e._1.toString + e._2.toString + e._3.toString)

  /**
    * Method to get all links from the graph of the semantic model.
    * This method is needed for Json serialization.
 *
    * @return List[HelperLink]
    */
  def getHelperLinks: List[HelperLink] = graph.edges
    .map(e =>
      HelperLink(id = e.id, source = e.from.id, target = e.to.id,
        label = e.ssdLabel.label, lType = e.ssdLabel.labelType, status = e.ssdLabel.status, prefix = e.ssdLabel.prefix)
    ) toList

  /**
    * Check if the semantic model is connected.
    */
  def isConnected: Boolean = graph.isConnected

  /**
    * Get the label of the node in the graph.
    *
    * @param nodeID Node id in the semantic model.
    * @return
    */
  def getNodeLabel(nodeID: NodeID): SsdLabel = {
    graph.nodes
      .find(n => n.id == nodeID) match {
        case Some(nn) =>
          nn.ssdLabel
        case _ =>
          throw TypeException("Wrong node id in the mappings -- it doesn't exist in the semantic model.")
    }
  }

  /**
    * Helper Function to get the uri of the node if it is of type ClassNode.
    *
    * @param node Node in the semantic model
    * @return
    */
  private def getClassUri(node: NodeT): Option[String] = {
    node.ssdLabel.labelType match {
      case "ClassNode" =>
        Some(node.ssdLabel.getURI)
      case _ =>
        None
    }
  }

  /**
    * Helper Function to get the uri of the link if it is of type DataPropertyLink or ClassInstanceLink.
    *
    * @param edge Link in the semantic model.
    * @return
    */
  private def getDataPropertyUri(edge: LinkT): Option[String] ={
    edge.ssdLabel.labelType match {
      case "DataPropertyLink" =>
        Some(edge.ssdLabel.getURI)
      case "ClassInstanceLink" =>
        Some(edge.ssdLabel.getURI)
      case _ =>
        None
    }
  }

  /**
    * Get domain and type of a data node.
    * Domain is the uri of the associated class node.
    * Type is the uri of the associated property link.
    *
    * @param nodeID Id of the node in the semanic model.
    * @return
    */
  def getDomainType(nodeID: NodeID): Option[(String, String)] = {
    graph.nodes
      .find(n => n.id == nodeID) match {
      case Some(nn) => {
        nn.ssdLabel.labelType match {
          case "DataNode" | "LiteralNode" =>
            // get the class node: it should be the inNeighbor of the DataNode
            val className: String = nn.inNeighbors
              .flatMap(n => getClassUri(n.asInstanceOf[NodeT]))
              .toList match {
                case List(s: String) => s
                case List() =>
                  logger.warn(s"ClassNode was not found for this DataNode $nodeID")
                  "" // we set the class uri to be empty string
                case _ =>
                  logger.error(s"DataNode $nodeID has more than 1 ClassNode.")
                  throw TypeException(s"DataNode $nodeID has more than 1 ClassNode.")
            }
            // get the property link: it should be the incoming link of the DataNode
            val propName: String = nn.incoming
              .flatMap(e => getDataPropertyUri(e.asInstanceOf[LinkT]))
              .toList match {
              case List(s: String) => s
              case _ =>
                logger.error(s"We cannot identify DataPropertyLink for DataNode $nodeID.")
                logger.debug(s"ClassName= $className")
                logger.debug(s"Data properties=${nn.incoming.flatMap(e => getDataPropertyUri(e.asInstanceOf[LinkT])).toList}")
                throw TypeException(s"We cannot identify DataPropertyLink for DataNode $nodeID.")
            }

            Some(className, propName)
          case _ =>
            None // domain and type make no sense for others
        }
      }
      case _ =>
        logger.error(s"Wrong node id $nodeID -- it doesn't exist in the semantic model.")
        throw TypeException(s"Wrong node id $nodeID -- it doesn't exist in the semantic model.")
    }
  }

  /**
    * Create Karma LabeledLink from our SsdLink
    *
    * @param e SsdLink
    * @param source Karma Node
    * @param target Karma Node
    * @param ontoManager Karma OnotologyManager
    * @return
    */
  private def createKarmaLink(e: LinkT, source: Node, target: Node, ontoManager: OntologyManager): LabeledLink = {
    // id of links in Karma: 'URI of source node'---'URI of link label'---'URI of target node'
//    val karmaID = s"${e.source.ssdLabel.getURI}---${e.ssdLabel.getURI}---${e.target.ssdLabel.getURI}"
    val karmaID = s"${source.getId}---${e.ssdLabel.getURI}---${target.getId}"
    e.ssdLabel.labelType match {
      case "DataPropertyLink" =>
        new DataPropertyLink(karmaID, new KarmaLabel(e.ssdLabel.getURI))
      case "ObjectPropertyLink" =>
        // object property type is defined from the ontology
        val objPropType: ObjectPropertyType = ontoManager
          .getObjectPropertyType(source.getUri,
            target.getUri,
            e.ssdLabel.getURI)
        new ObjectPropertyLink(karmaID, new KarmaLabel(e.ssdLabel.getURI), objPropType)
      case "SubClassLink" =>
        new SubClassLink(karmaID)
      case "ClassInstanceLink" =>
        new ClassInstanceLink(karmaID) // FIXME: LinkKeyInfo needs to be initialized...
      case "ColumnSubClassLink" =>
        logger.error(s"Unsupported link type ${e.ssdLabel.labelType}")
        throw TypeException(s"Unsupported link type ${e.ssdLabel.labelType}")
      case "DataPropertyOfColumnLink" => // no idea what this is for
        logger.error(s"Unsupported link type ${e.ssdLabel.labelType}")
        throw TypeException(s"Unsupported link type ${e.ssdLabel.labelType}")
      case "ObjectPropertySpecializationLink" => // no idea what this is for
        logger.error(s"Unsupported link type ${e.ssdLabel.labelType}")
        throw TypeException(s"Unsupported link type ${e.ssdLabel.labelType}")
      // there are still some COmpactType links
      case _ =>
        logger.error(s"Unsupported link type ${e.ssdLabel.labelType}")
        throw TypeException(s"Unsupported link type ${e.ssdLabel.labelType}")
    }
  }

  /**
    * Helper function to get Karma Origin for the SemanticType.
    * If ssdLabel.labelType is not one of the values in the enum for KarmaOrigin,
    * we set the origin to the default value KarmaOrigin.User.
    *
    * @param ssdLabel Label
    * @return
    */
  private def getKarmaOrigin(ssdLabel: SsdLabel): KarmaOrigin = {
    Try{
      KarmaOrigin.valueOf(ssdLabel.labelType)
    } match {
      case Success(ko: KarmaOrigin) => ko
      case _ => KarmaOrigin.User
    }
  }

  /**
    * Create a map from our node ids to Karma ColumnNodes.
    *
    * @param ssdMappings Mappings of attributes to nodes in the semantic model.
    * @return
    */
  private def columnNodeMap(ssdMappings: SsdMapping): Map[NodeID, Node] = {
    ssdMappings.mappings.map {
      case (attrID, nodeID) => {
        val nodeLabel: SsdLabel = getNodeLabel(nodeID)

        // NOTE: we create ids for Karma by adding HN to the attribute id
        // NOTE: since attribute ids have to be unique, created ids for Karma ColumnNodes will be unique as well.
        // hNodeId and id in Karma are equal for ColumnNode
        val hNodeId = s"HN$attrID"

        // we put the columnName equal to the label -- though it's not correct, it's not relevant for our side
        val colNode = new ColumnNode(hNodeId
          , hNodeId
          , nodeLabel.label
          , null  // ColumnNodes have empty KarmaLabels
          , null) // Karma guys used null for language everywhere

        // set isForced property based on node status
        // FIXME: is that correct setup?
        // forced nodes will be included always in steinernodes even if they don't have semantic type.
        // so, we have to be careful with it!
        colNode.setForced(nodeLabel.status.contains("User"))

        // we need to set now userSemanticTypes for this column
        // to this end we need to identify domain and type of the property
        getDomainType(nodeID) match {
          case Some((classURI: String, propURI: String)) =>
            val newType: KarmaSemanticType = new KarmaSemanticType(hNodeId
              , new KarmaLabel(propURI)
              , new KarmaLabel(classURI)
              , getKarmaOrigin(nodeLabel)
              , 1.0) // we set the confidence for user semantic type to 1 --- meaning it's enforced by the user
            colNode.assignUserType(newType)
          case None =>
            logger.error("ColumnNode cannot be created since DataNode is not properly defined.")
            throw TypeException("ColumnNode cannot be created since DataNode is not properly defined.")
        }

        // TODO: do we need to set learnedSemanticTypes explicitly to []?
//        colNode.setLearnedSemanticTypes(new util.LinkedList[KarmaSemanticType]())
        nodeID -> colNode
      }
    }
  }

  /**
    * Helper method to obtain a map from uris to the list of nodes which have a specific uri.
 *
    * @return
    */
  private def indexUriLabel: Map[String,List[NodeID]] = {
    graph.nodes.map {
      node => (node.ssdLabel.getURI, node.id)
    }.toList
      .groupBy(_._1)
      .map {
        case (k,v) => (k, v.map {
          case (k1,v1) => v1
        })
      }
  }

  /**
    * Helper method to construct Karma id for the node.
    * This method should be called for InternalNodes.
    * Since Karma tool uses URIs as ids for nodes in the semantic model,
    * we need to make sure that nodes which correspond to the same ontology class still have unique URIs.
    * For this purpose we index them.
    *
    * @param nodeID Id of the node
    * @param uriLab Uri label of the node
    * @return
    */
  private def getInternalNodeId(nodeID: NodeID, uriLab: String): String = {
    // FIXME: we might run into issues when automatically assigning indexes for class nodes
    // NOTE: The order of nodes in the semantic model matters to this end!
    // we add 1 to index since in Karma they index class instances starting from 1
    uriLab + (indexUriLabel(uriLab).indexOf(nodeID) + 1).toString
  }

  /**
    * Create a map from our node ids to Karma Nodes which are not ColumnNode.
    * For ClassNode we have to be careful with karma ids for nodes in the graph:
    * we need to add integers at the end which would ensure uniqueness of nodes.
    *
    * @param ssdMappings Mappings of attributes to nodes in the semantic model.
    * @return
    */
  private def otherNodeMap(ssdMappings: SsdMapping): Map[NodeID,Node] = {
    graph.nodes.filter {
      node => !ssdMappings.mappings.values.toSet.contains(node.id) // we get all nodes which are not ColumnNode
    }.flatMap {
      node =>
        node.ssdLabel.labelType match {
          case "ClassNode" =>
            // we add integer number at the end of URI;
            // this number corresponds to how many ClassNodes of the same URI occur in the semantic model
            val newNode = new InternalNode(getInternalNodeId(node.id, node.ssdLabel.getURI)
              , new KarmaLabel(node.ssdLabel.getURI))
            // set isForced property based on node status
            newNode.setForced(node.ssdLabel.status.toLowerCase.contains("user"))
            Some(node.id -> newNode)
          case "LiteralNode" =>
            val newNode = new LiteralNode(node.id.toString
              , ""                                    // FIXME: LiteralNodes need default value to be initialized
              , new KarmaLabel(node.ssdLabel.getURI)  // TODO: this should be datatype from XSD
              , null                                  // language is null mainly in Karma
              , false)                                //TODO: not clear what to do with isUri
            newNode.setForced(node.ssdLabel.status.toLowerCase.contains("user"))
            Some(node.id -> newNode)
          case _ =>
            logger.warn(s"Unsupported node type for $node")
            None // we filter out all nodes which are not ClassNode or LiteralNode...
        }
    } toMap
  }

  /**
    * Convert to Karma-like representation of the semantic model.
    * We need the Karma ontology manager to define object property types.
    * TODO: check semantic types and all uris using ontology manager.
    *
    * @param ssdMappings SSDMapping needed to identify ColumnNode
    * @param ontoManager OntologyManager from Karma
    */
  def toKarmaGraph(ssdMappings: SsdMapping, ontoManager: OntologyManager): KarmaGraph = {
    logger.debug("Converting SemanticModel to KarmaGraph...")
    // Converting node types:
    //  Nodes which are in ssdMappings -> ColumnNode
    //  Nodes of type ClassNode -> InternalNode
    //  Nodes of type LiteralNode -> LiteralNode
    //  all other types -> None
    val karmaGraph = new DirectedWeightedMultigraph[Node, LabeledLink](classOf[LabeledLink])

    val karmaNodeMap = columnNodeMap(ssdMappings) ++ otherNodeMap(ssdMappings)

    // adding nodes to karmaGraph
    karmaNodeMap foreach {
      case (nodeID, karmaNode) => karmaGraph.addVertex(karmaNode)
    }

    // Karma puts a weird id on links: it's a combination of 3 URIs concatenated with '---'
    // 'URI of source node'---'URI of link label'---'URI of target node'
    graph.edges
      .foreach {
        e => {
          val source = karmaNodeMap(e.source.id)
          val target = karmaNodeMap(e.target.id)
          val edge = createKarmaLink(e.asInstanceOf[LinkT], source, target, ontoManager)
          // change the status of the edge
          // we need to be careful since it influences the edge weight later on
          edge.setStatus(AlignLinkStatus.valueOf(e.ssdLabel.status))
          // edgeWeight exists for DefaultLink, not LabeledLink
          karmaGraph.addEdge(source, target, edge)
      }
    }
    KarmaGraph(karmaGraph)
  }
}

/**
  * custom JSON serializer for the Semantic Model
  * we need two fields: nodes and links
  */
case object SemanticModelSerializer extends CustomSerializer[SemanticModel](
  format => (
    {
      case jv: JValue =>
        val nodes: Map[NodeID,SsdNode] = HelperJSON.parseOption[List[SsdNode]]("nodes",jv) match {
          case Success(Some(nList)) =>
            nList.map(node => node.id -> node) toMap
          case Success(None) =>
            throw TypeException("Nodes are absent.")
          case Failure(err) =>
            throw TypeException(s"Failed to extract nodes. ${err.getMessage}")
        }
        val hLinks: List[HelperLink] = HelperJSON.parseOption[List[HelperLink]]("links",jv) match {
          case Success(Some(eList)) =>
            eList
          case Success(None) =>
            throw TypeException("Links are absent.")
          case Failure(err) =>
            throw TypeException(s"Failed to extract links. ${err.getMessage}")
        }
        val links: List[SsdLink[SsdNode]] = hLinks.map(
          hLink => {
            val n1 = nodes.getOrElse(hLink.source,
              throw TypeException("Wrong link: non-existent source."))
            val n2 = nodes.getOrElse(hLink.target,
              throw TypeException("Wrong link: non-existent target."))
            SsdLink(n1, n2
              , hLink.id
              , SsdLabel(hLink.label, hLink.lType, hLink.status, hLink.prefix))
        })
        val g: Graph[SsdNode,SsdLink] = Graph() // we make it immutable!!!
        SemanticModel(g ++ links)
    }, {
    case sm: SemanticModel =>
      implicit val formats = DefaultFormats + SsdNodeSerializer + HelperLinkSerializer
      val jvNodes = JArray(sm.getNodes.map(node => Extraction.decompose(node)))
      val jvLinks = JArray(sm.getHelperLinks.map(link => Extraction.decompose(link)))
      JObject(JField("nodes",jvNodes) :: JField("links",jvLinks) :: Nil)
  })
)


