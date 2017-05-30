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
import java.util

import org.jgrapht.graph.DirectedWeightedMultigraph
import com.typesafe.scalalogging.LazyLogging

import scala.language.postfixOps
import scala.language.implicitConversions
import scala.collection.JavaConverters._
import java.lang.{Double => jDouble, Long => jLong}

import scala.util.{Failure, Success, Try}
import edu.isi.karma.modeling.alignment.learner._
import edu.isi.karma.modeling.alignment.{Alignment, GraphUtil, LinkIdFactory, SemanticModel => KarmaSSD}
import edu.isi.karma.rep.alignment.SemanticType.Origin
import edu.isi.karma.rep.alignment.{ColumnNode, InternalNode, LabeledLink, Node, SemanticType, Label => KarmaLabel}
import au.csiro.data61.modeler.ModelerConfig
import au.csiro.data61.types._
import au.csiro.data61.types.ColumnTypes.ColumnID
import au.csiro.data61.types.SsdTypes.{AttrID, OctopusID}
import au.csiro.data61.types.Exceptions._

/**
  * As input we give a ssd and a list of predicted semantic types for a data source.
  * As output we get a list of ssd with associated scores.
  *
  * @param karmaWrapper Object which holds initialized Karma parameters
  */
case class KarmaSuggestModel(karmaWrapper: KarmaParams) extends LazyLogging {

  val defaultNumSemanticTypes = 4

  /**
    * needed for alignment construction when using Karma
    * needs to be different from KarmaBuildAlignmentGraph
    */
  private var suggestModelLearningGraph = ModelLearningGraph
    .getInstance(karmaWrapper.karmaWorkspace.getOntologyManager, ModelLearningGraphType.Compact)

  /**
    * Read in most recent Karma stuff from the disk.
    * FIXME: how to properly keep track of the state of Karma stuff???
    */
  private def karmaInitialize() = {
    //karmaWrapper = KarmaParams()
    logger.debug(s"Initializing suggestModelLearningGraph")
    suggestModelLearningGraph = ModelLearningGraph
      .getInstance(karmaWrapper.karmaWorkspace.getOntologyManager, ModelLearningGraphType.Compact)
    logger.debug(s"suggestModelLearningGraph: ${suggestModelLearningGraph.getGraphBuilder.getGraph.vertexSet.size} nodes, " +
      s"${suggestModelLearningGraph.getGraphBuilder.getGraph.edgeSet.size} links.")
  }

  // val model: SemanticModel = modelLearner.getModel -- returns only the top semantic model
  // util.List[SortableSemanticModel] hypothesisList = modelLearner.hypothesize(true, NUM_SEMANTIC_TYPES) -- returns a list of learnt sortable semantic models
  // scores....
  // SortableSemanticModel : cost,
  //                         linkCoherence
  // SortableSemanticModel.steinerNodes: getScore,
  //                                     getConfidence,
  //                                     getCoherence,
  //                                     getSizeReduction -- private!!!,
  //                                     getNodesCount,
  //                                     getScoreDetailsString

  // schema matcher api returns DataSetPrediction

  /**
    * Associate an SSDAttribute with a prediction object from DataSetPrediction which is returned by matcher.
    * NOTE: DataSetPrediction contains predictions per matcher:ColumnID.
    * To avoid problems with correspondence between modeller:AttrID and matcher:ColumnID,
    * an additional map attrToColMap is used.
    * In case there are no predictions for the attribute, an empty map is returned.
    *
    * @param attr SSDAttribute for which predictions need to be determined
    * @param dsPredictions Matcher DataSetPrediction object which contains predictions for columns in the dataset
    * @param attrToColMap Mapping of modeller:attributes to matcher:columns
    * @return
    */
  private def getPredictedSemanticTypes(attr: SsdAttribute
                                        , dsPredictions: DataSetPrediction
                                        , attrToColMap: Map[AttrID,ColumnID]
                                       ) : Map[String, Double] = {
    logger.debug(s"Looking up predicted semantic types for attribute ${attr.id}: ${attr.name}")
    // if this fails, it means that attrToColMap is not set properly
    val colID = attrToColMap(attr.id).toString
    Try {
      dsPredictions.predictions(colID).scores
    }  match {
      case Success(scores: Map[String, Double]) =>
        scores
      case _ => Map() // there are no predictions for this attribute
    }
  }

  /** Convert the list of predicted semantic types for the attribute to the Karma-like list of semantic types.
    * In case there are no predicted semantic types for the attribute, an empty list is returned.
    *
    * @param hNodeId String which identifies columns in Karma and corresponds to 'HN'+str(modeller:AttrID)
    * @param predictedSemanticTypes Map of predictedSemanticTypes = scores from matcher:DataSetPrediction
    * @param semanticTypeMap Mapping of matcher:classLabels to URIs.
    * @return
    */
  private def getLearntSemanticTypes(hNodeId: String
                                     , predictedSemanticTypes: Map[String, Double]
                                     , semanticTypeMap: Map[String, String]
                                    ) : util.List[SemanticType] = {
    logger.debug(s"Getting learnt semantic types for Karma ColumnNode $hNodeId")
    val scalaRes: List[SemanticType] = predictedSemanticTypes.toList
      .map{
        predSemType =>
          // predSemType should be of the format: class---property
          // where class corresponds to the label of the corresponding ClassNode in the ontology
          // and property corresponds to the label of the corresponding DataNode in the ontology
          val parts = predSemType._1.split("---")
          if(parts.size != 2) {
            logger.error(s"Schema Matcher class label is not set properly: ${predSemType._1}")
            throw ModelerException(s"Schema Matcher class label is not set properly: ${predSemType._1}")
          }
          // if class from predSemType is not in the semanticTypeMap,
          // we take the default namespace from the TypeConfig and concatenate it with class.
          val domainUri = semanticTypeMap.getOrElse(parts(0), TypeConfig.DefaultNamespace) + parts(0)
          val propertyUri = semanticTypeMap.getOrElse(parts(1), TypeConfig.DefaultNamespace) + parts(1)
          logger.debug(s"$hNodeId: (DomainURI, PropertyURI) = ($domainUri,$propertyUri)")
          val domainLabel: KarmaLabel = karmaWrapper.karmaWorkspace.getOntologyManager.getUriLabel(domainUri)
          val propertyLabel: KarmaLabel = karmaWrapper.karmaWorkspace.getOntologyManager.getUriLabel(propertyUri)
          // NOTE: I've added DINT to Origin enum in Karma code
          (hNodeId, domainLabel, propertyLabel, Origin.DINT, predSemType._2)
      }.filter {
      case (idStr, domainLabel, propertyLabel, origin, conf) =>
        domainLabel != null && propertyLabel != null // we filter out predictions with improper labels
      }
      .map {
        case (idStr, domainLabel, propertyLabel, origin, conf) =>
          logger.debug(s"---$idStr confidence: $conf")
          new SemanticType(idStr,
            propertyLabel,
            domainLabel,
            origin,
            conf)
    }
    // some craze java <-> scala conversion... making it explicit!
    val javaRes: util.List[SemanticType] = scalaRes.asJava
    javaRes
//    res.asJava.asInstanceOf[util.List[SemanticType]]
  }

  /**
    * Convert not mapped attributes in the ssd to Karma-like list of ColumnNode.
    *
    * @param ssd SemanticSourceDesc for which we need to learn the alignment.
    * @param dsPredictions Matcher DataSetPrediction object which contains predictions for columns in the dataset.
    * @param semanticTypeMap Mapping of matcher:classLabels to URIs.
    * @param attrToColMap Mapping of modeller:attributes to matcher:columns.
    * @return
    */
  private def getKarmaNotMappedAttributes(ssd: Ssd
                                  , dsPredictions: DataSetPrediction
                                  , semanticTypeMap: Map[String, String]
                                  , attrToColMap: Map[AttrID,ColumnID]
                                 ): List[ColumnNode] = {
    logger.info("Converting not mapped attributes to Karma ColumnNodes")
    val columnNodes: util.List[ColumnNode] = new util.LinkedList()
    // we need to get attributes which are not in the mappings -- they need to be added as sourceColumns to Karma
    ssd.notMappedAttributes foreach {
      attr =>
        // NOTE: we create ids for Karma by adding HN to the attribute id
        // NOTE: since attribute ids have to be unique, created ids for Karma ColumnNodes will be unique as well.
        // hNodeId and id in Karma are equal for ColumnNode, but not in the generated Karma models...
        val hNodeId = s"HN${attr.id}"
        // ColumnNode will have status NotAssigned
        // FIXME: we put the columnName equal to the name unlike the SemanticModel
        val colNode = new ColumnNode(hNodeId
          , hNodeId
          , attr.name
          , null  // ColumnNodes have empty KarmaLabels
          , null) // Karma guys used null for language everywhere

        // set learnt semantic types based on predictions
        // NOTE: we search for matcher->ColumnID for modeller->AttrID in attrToColMap
        val predictedSemanticTypes: Map[String, Double] = getPredictedSemanticTypes(attr
          , dsPredictions
          , attrToColMap)

        val learntSemanticTypes: util.List[SemanticType] = getLearntSemanticTypes(hNodeId
          , predictedSemanticTypes
          , semanticTypeMap)
        // if there are no user semantic types and only learntSemanticTypes,
        // then the SemanticTypeStatus of this node should be set to AutoAssigned
        if (!learntSemanticTypes.isEmpty){
          colNode.setLearnedSemanticTypes(learntSemanticTypes)
          colNode.includeInAutoModel()
        }

        columnNodes.add(colNode)
    }
    logger.debug(s"SSD contains ${columnNodes.size} source columns which are not mapped.")
    columnNodes.asScala.toList
  }


  /**
    * Set-up properly column nodes in the alignment.
    *
    * @param sourceColumns List of ColumnNodes which need to be set-up in the alignment
    * @param alignmentGraph Alignment
    * @return
    */
  private def setAlignmentSourceColumns(sourceColumns: List[ColumnNode]
                                        , alignmentGraph: Alignment
                                       ): Alignment = {
    logger.info("Setting not mapped attributes in the alignment...")
    logger.debug(s"---Alignment1 sourcecolnodes: ${alignmentGraph.getSourceColumnNodes.size}")
    // copy alignment
    val updatedAlignment = alignmentGraph.getAlignmentClone

    // setting source columns in alignmentGraph
    sourceColumns.foreach {
      colNode: ColumnNode =>
        logger.debug(s"***ColNode name=${colNode.getColumnName}, SemanticTypeStatus=${colNode.getSemanticTypeStatus}")
        val cn: ColumnNode = Option(updatedAlignment.getColumnNodeByHNodeId(colNode.getHNodeId)) match {
          case Some(mappedColNode: ColumnNode) =>
            // column node is already in the alignment
            mappedColNode
          case None =>
            logger.debug(s"***adding colnode to alignment ${colNode.getHNodeId}")
            updatedAlignment.addColumnNode(colNode.getHNodeId,
              colNode.getColumnName,
              colNode.getRdfLiteralType,
              colNode.getLanguage)
        }
        logger.debug(s"Column node mapped to the alignment...$cn")
        logger.debug(s"Column node learnt semantic types...${Option(colNode.getLearnedSemanticTypes)}")
        Option(colNode.getLearnedSemanticTypes) match {
          case Some(semTypes) =>
            logger.debug(s"***Adding learnt semantic types!")
            cn.setLearnedSemanticTypes(semTypes)
            // we set ColumnNodeSemanticTypeStatus to "AutoAssigned" in case there are learnt semantic types
            cn.includeInAutoModel()
            colNode.includeInAutoModel()
          case None =>
            logger.debug(s"***No learnt semantic types!")
            cn.setLearnedSemanticTypes(new util.LinkedList[SemanticType]())
        }
        // we set ColumnNodeSemanticTypeStatus to "UserAssigned" in case there are user assigned types
        colNode.getUserSemanticTypes.asScala.foreach(cn.assignUserType)
      }
    logger.debug(s"---Alignment2 sourcecolnodes: ${updatedAlignment.getSourceColumnNodes.size}")
    logger.debug(s"---Alignment2 sourcecolnodes: ${updatedAlignment.getSourceColumnNodes.asScala.map(node => node.getSemanticTypeStatus)}")
    updatedAlignment
  }

  /**
    * Method which aligns the suggested SortableSemanticModel by Karma.
    * This alignment primarily affects the node ids of the ColumnNodes.
    * Also, if userSemanticTypes are missing, it will assign them to ColumnNodes.
    * We need these changes so that we can properly convert Karma suggestion to our SSD.
    *
    * @param sortableKarmaSM SortableSemanticModel returned by Karma.
    * @param alignmentGraph Alignment
    * @return
    */
  private def convertSortableKarmaSM(sortableKarmaSM: SortableSemanticModel
                                     , alignmentGraph: Alignment
                                    ): Alignment ={
    // first we need to get a proper KarmaSSD based on the returned SortableSemanticModel
    val semanticTypes: util.List[SemanticType] = new util.LinkedList[SemanticType]
    val updatedAlignment: Alignment = alignmentGraph.getAlignmentClone
    // we need to update the returned karma model because of crazy node mappings in Karma
    updatedAlignment.updateAlignment(sortableKarmaSM, semanticTypes)
    val semanticTypeMap: Map[String, SemanticType] = semanticTypes.asScala
      .map {
        semType => semType.getHNodeId -> semType
      } toMap

    // we need to assign user semantic types to ColumnNodes since this is required for further conversion to our SSD
    updatedAlignment.getSteinerTree.vertexSet.asScala
      .foreach {
        case cn: ColumnNode =>
          semanticTypeMap.getOrElse(cn.getHNodeId, None) match {
            case semType: SemanticType =>
              Option(cn.getUserSemanticTypes) match {
                case None => cn.assignUserType(semType)
                case Some(st) =>
                  if (st.isEmpty) {cn.assignUserType(semType)}
              }

            case _ => logger.debug("Ignoring this node.")
          }
        case _ => logger.debug("Ignoring this node.")
      }
    updatedAlignment
  }


  /**
    * Convert the suggestion returned by Karma to our data structure.
    *
    * @param ssd SemanticSourceDesc for which we need to learn the alignment.
    * @param sortableKarmaSM SortableSemanticModel found by Karma for this ssd.
    * @param rank Rank of the sortableKarmaSM as indicated by Karma.
    * @return
    * @throws ModelerException when semantic scores cannot be calculated
    */
  private def convertSuggestion(ssd:Ssd
                        , sortableKarmaSM: SortableSemanticModel
                        , rank: Int
                        , alignmentGraph: Alignment
                       ) : (Ssd, SemanticScores) = {
    // debugging...
//    sortableKarmaSM.writeJson(Paths.get(ModelerConfig.KarmaDir, s"karma_sortableSM_$rank.json").toString)

    // to construct the new SemanticSourceDesc, we update the existing one by using Karma response
    // first we need to get a proper KarmaSSD based on the returned SortableSemanticModel
    val semanticTypes: util.List[SemanticType] = new util.LinkedList[SemanticType]
    val updatedAlignment = convertSortableKarmaSM(sortableKarmaSM, alignmentGraph)

    //debugging...
//    GraphUtil.exportJson(GraphUtil.asDefaultGraph(updatedAlignment.getSteinerTree),
//      Paths.get(ModelerConfig.KarmaDir, s"karma_alignment_steinertree_$rank.json").toString, true, true)
//    sortableKarmaSM.writeGraphviz(
//      Paths.get(ModelerConfig.KarmaDir, s"karma_alignment_steinertree_$rank.dot").toString, false,false)

    // we update the input ssd with the suggestion from Karma
    // for this purpose, we have to get the steiner tree from the updated alignment
    val  model: Ssd = ssd.update(KarmaGraph(updatedAlignment.getSteinerTree))

    Option(sortableKarmaSM.getSteinerNodes) match {
      case Some(_) =>
        val scores = SemanticScores(linkCost = sortableKarmaSM.getCost,
          linkCoherence = sortableKarmaSM.getLinkCoherence.getCoherenceValue,
          nodeConfidence = sortableKarmaSM.getConfidenceScore,
          nodeCoherence = sortableKarmaSM.getSteinerNodes.getCoherence.getCoherenceValue,
          sizeReduction = sortableKarmaSM.getSteinerNodes.getSizeReduction,
          nodeCoverage = sortableKarmaSM.getColumnNodes.size.toDouble / ssd.attributes.size,
          karmaScore = sortableKarmaSM.getScore,
          karmaRank = rank)
        logger.debug(s"Suggested semantic model has $scores")
        (model,scores)
      case None =>
        logger.error("Error in the suggested model. SortableSemanticModel has null steiner nodes.")
        throw ModelerException("Error in the suggested model. SortableSemanticModel has null steiner nodes.")
    }
  }

  /**
    * Helper method to update links in the alignment graph based on the semantinc model.
    * If the link from the semantic model is already present in the alignment, we update its status.
    * Otherwise, we add it to the alignment.
    * Here the idea is that if the semantic model is partially aligned, we want to include this info when constructing
    * the Steiner tree.
    *
    * @param updatedAlignment Current alignment.
    * @param karmaModel Karma Semantic Model object which needs to be incorporated into alignment.
    * @param modelToAlignmentNode Mapping from nodes in KarmaModel to nodes in the alignment.
    * @return
    */
  private def updateAlignmentLinks(updatedAlignment: Alignment
                                   , karmaModel: KarmaSSD
                                   , modelToAlignmentNode: util.HashMap[Node,Node]
                                  ) = {
    // updating links
    // previously, I tried to copy the current alignment and change links, but there was some weird error with copying
    // strings, so I just modify the state of updatedAlignment -- it's mutable in any case
    karmaModel.getGraph.edgeSet.asScala.foreach {
      link =>
        modelToAlignmentNode.get(link.getSource) match {
          case source: Node =>
            val target = modelToAlignmentNode.get(link.getTarget)
            val linkId = LinkIdFactory.getLinkId(link.getUri, source.getId, target.getId)
            val newLink: LabeledLink = link.copy(linkId)
            updatedAlignment.getGraphBuilder.addLink(source, target, newLink) // returns false if link already exists
            updatedAlignment.changeLinkStatus(newLink.getId, newLink.getStatus) // even if the link already exists, we need to make sure that the status changes
            target match {
              case t: ColumnNode =>
                // we assign user semantic type explicitly for the node, and NOTE: we preserve hNodeId from the original node!!!
                val semType = new SemanticType(link.getTarget.asInstanceOf[ColumnNode].getHNodeId,
                  newLink.getLabel, source.getLabel, SemanticType.Origin.User, 1.0)
                t.assignUserType(semType)
              case _ =>
                logger.debug(s"-----updatedAlign: missing target $target")
            }
          case _ =>
            logger.debug(s"-----updatedAlign: missing source ${link.getSource}")
        }
    }
    logger.debug(s"Size of the updated alignment graph: ${updatedAlignment.getGraphNodes.size} nodes" +
      s", ${updatedAlignment.getGraph.edgeSet.size}")
    logger.debug(s"Size of the upated alignment graphBuilder: ${updatedAlignment.getGraphBuilder.getGraph.vertexSet.size}" +
      s" nodes, ${updatedAlignment.getGraphBuilder.getGraph.edgeSet.size}")
    logger.debug(s"---UpdatedAlignment sourcecolnodes: ${updatedAlignment.getSourceColumnNodes.size}")
  }

  def updateInternalNode(n: InternalNode,
                         modelToAlignmentNode: util.HashMap[Node, Node],
                         alignNodeMappings: util.HashMap[Node, Node],
                         updatedAlignment: Alignment): Unit = {

    val alignNode = alignNodeMappings.get(n)

    Option(updatedAlignment.getNodeById(alignNode.getId)) match {
      case Some(iNode: InternalNode) =>
        iNode.setForced(true) // this way we ensure that this node is in Steiner tree
        modelToAlignmentNode.put(n, iNode)
      case None =>
        // we add the node to the alignmentGraph first and then put it into the map
        val newNode = updatedAlignment.addInternalNode(alignNode.getLabel)
        newNode.setForced(true) // this way we ensure that this node is in Steiner tree
        modelToAlignmentNode.put(n, newNode)
      case _ =>
        throw new Exception("Unknown node mapping found")
    }
  }

  def updateAlignmentNode(n: ColumnNode,
                          modelToAlignmentNode: util.HashMap[Node, Node],
                          alignNodeMappings: util.HashMap[Node, Node],
                          updatedAlignment: Alignment): Unit = {
    alignNodeMappings.get(n) match {
      case cn: ColumnNode =>
        Option(updatedAlignment.getNodeById(cn.getId)) match {
          case Some(availNode: ColumnNode) =>
            logger.debug("Node already in the alignment graph")
          case None =>
            updatedAlignment.getGraphBuilder.addNodeAndUpdate(cn)
          case _ =>
            throw new Exception("Unknown alignment node found.")
        }
        if (!updatedAlignment.getSourceColumnNodes.contains(cn)) {
          updatedAlignment.getSourceColumnNodes.add(cn) // this way we ensure that this node is in Steiner tree
        }
        modelToAlignmentNode.put(n, cn)
      case _ =>
        logger.debug("ColumnNode not there.")
    }
  }

  /**
    * Helper method to update alignment based on the semantic model.
    * We need to include nodes and links as well as change characteristics of the existing ones.
    *
    * @param alignmentGraph Current alignment
    * @param karmaModel Karma Semantic Model object which needs to be incorporated into alignment
    * @param alignNodeMappings Mappings for nodes in karmaModel to the ndes in the alignmentGraph
    * @return
    */
  private def updateAligment(alignmentGraph: Alignment,
                             karmaModel: KarmaSSD,
                             alignNodeMappings: util.HashMap[Node,Node]
                            ): Alignment = {
    logger.info("Updating alignment graph based on the info in the semantic model.")
    logger.debug(s"---Alignment sourcecolnodes before update: ${alignmentGraph.getSourceColumnNodes.size}")
    // copy alignment
    val updatedAlignment = alignmentGraph.getAlignmentClone

    val modelToAlignmentNode = new util.HashMap[Node,Node]()
    // updating nodes
    karmaModel.getGraph.vertexSet.asScala.foreach {

      case n: InternalNode =>
        updateInternalNode(n, modelToAlignmentNode, alignNodeMappings, updatedAlignment)

      case n: ColumnNode =>
        updateAlignmentNode(n, modelToAlignmentNode, alignNodeMappings, updatedAlignment)
      case _ =>
        throw new Exception("Unknown node found in alignment")
    }

    // updating links
    updateAlignmentLinks(updatedAlignment, karmaModel, modelToAlignmentNode)
    updatedAlignment
  }

  /**
    * Helper method to obtain mappings from SSD nodes to Alignment graph nodes.
    * For example, column nodes may have different ids in the converted SSD and the Alignment graph.
    * Here, we use Karma method which will match such cases.
    *
    * @param karmaSSD Input SSD converted to Karma graph.
    * @return
    */
  private def getAlignNodeMappings(karmaSSD: KarmaSSD): util.HashMap[Node,Node] = {
    logger.info("Obtaining mappings from the nodes of the new semantic model to the alignment graph.")
    // we need the following to obtain correct mappings for nodes in karmaSSD to the alignment graph
    suggestModelLearningGraph.addModelAndUpdate(
      karmaSSD,
      PatternWeightSystem.JWSPaperFormula // TODO: understand the difference between formulas
    )
    suggestModelLearningGraph.setLastUpdateTime(java.lang.System.currentTimeMillis)

    val internalMap = suggestModelLearningGraph
      .asInstanceOf[ModelLearningGraphCompact]
      .getInternalNodeMapping(karmaSSD)

    val colMap = suggestModelLearningGraph
      .asInstanceOf[ModelLearningGraphCompact]
      .getColumnNodeMapping(karmaSSD, internalMap)

    val alignNodeMappings = new util.HashMap[Node,Node]()
    alignNodeMappings.putAll(internalMap)
    alignNodeMappings.putAll(colMap)
    // debugging
//    alignNodeMappings.asScala.foreach { m => logger.debug(s"----alignNodeMapping: ${m._1.getId} -> ${m._2.getId} ") }

    alignNodeMappings
  }

  /**
    * Construct the initial alignment graph for the ssd.
    * It is different from the Alignment Graph in KarmaBuildAlignmentGraph in the sense that we
    * include the incomplete input semantic source description into it.
    *
    * @param ssd SemanticSourceDesc for which we need to learn the alignment.
    * @return
    */
  private def getAlignment(ssd: Ssd): Alignment = {
    logger.info(s"Setting up initial alignment of SSD ${ssd.id}")
    val alignmentGraph = new Alignment(karmaWrapper.karmaWorkspace.getOntologyManager)
    //debugging...
//    GraphUtil.exportJson(alignmentGraph.getGraphBuilder.getGraph,
//      Paths.get(ModelerConfig.KarmaDir, s"karma_alignment_graph.json").toString, true, true)

    logger.info("Converting input SSD to Karma-like SemanticModel")
    // get initial graph
    val updatedAlignmentGraph = ssd.toKarmaSemanticModel(karmaWrapper.karmaWorkspace.getOntologyManager) match {
      case Some(karmaSM) =>
        val karmaSSD = new KarmaSSD(ssd.id.toString, karmaSM.karmaModel.getGraph)
        karmaSSD.setName(ssd.name)
        updateAligment(alignmentGraph, karmaSSD, getAlignNodeMappings(karmaSSD))
      // if there is no semantic model, we do not need to update the alignment
      case None =>
        logger.debug("Alignment not updated.")
        alignmentGraph.getAlignmentClone
    }

    //debugging...
//    GraphUtil.exportJson(updatedAlignmentGraph.getGraphBuilder.getGraph,
//      Paths.get(ModelerConfig.KarmaDir, s"karma_updated_alignment_graph.json").toString, true, true)

    updatedAlignmentGraph
  }

  /**
    * Helper method to get TopK Karma suggestions for ssd.
    *
    * @param ssd Semantic Source Description
    * @param alignmentGraph Karma Alignment
    * @param numSemanticTypes Integer which indicates how many top semantic types to keep
    * @return
    */
  private def getKarmaSuggestions(ssd: Ssd
                                  , alignmentGraph: Alignment
                                  , numSemanticTypes: Integer
                                 ): List[(Ssd, SemanticScores)] = {
    logger.debug("******************************************")
    logger.debug(s"Alignment graph: ${alignmentGraph.getGraph.vertexSet.size} nodes, " +
      s"${alignmentGraph.getGraph.edgeSet.size} links.")

    val steinerNodes = alignmentGraph.computeSteinerNodes()
    logger.debug(s"${steinerNodes.size} Steiner nodes are obtained.")
    if(steinerNodes.isEmpty) {
      logger.error("Steiner nodes are missing.")
      throw ModelerException("Steiner nodes are missing. Learning is impossible.")
    }

    // TODO: figure out difference between modelLearners
    //    if (modelingConfiguration.getKnownModelsAlignment())
    //      modelLearner = new ModelLearner(alignment.getGraphBuilder(), steinerNodes);
    //    else
    //      modelLearner = new ModelLearner(ontologyManager, alignment.getLinksByStatus(LinkStatus.ForcedByUser), steinerNodes);
    logger.debug("Initializing the model learner")
    val modelLearner = new ModelLearner(alignmentGraph.getGraphBuilder, steinerNodes)

    // returns a list of learnt sortable semantic models
    // TODO: what is the meaning of the first argument in the method?
    val hypothesisList: List[SortableSemanticModel] = modelLearner
      .hypothesize(true, numSemanticTypes).asScala.toList
    logger.info(s"${hypothesisList.size} suggestions by Karma")

    // FIXME: unnecessary -- sorting the list (it's already sorted)
    // FIXME: isomorphic graphs are returned as well + graphs with extra InternalNode
    hypothesisList.sortWith{{(leftSM,rightSM) => leftSM.compareTo(rightSM) == -1}}

    hypothesisList
      .zipWithIndex
      .map {
        case (sortableKarmaSM: SortableSemanticModel, rank: Int) =>
          logger.info(s"Converting suggestion $rank for SSD ${ssd.id}")
          convertSuggestion(ssd, sortableKarmaSM, rank, alignmentGraph)
      }
  }

  /**
    * Suggest semantic models for the input ssd.
    * This method uses Karma tool, preloaded ontologies and known semantic source descriptions.
    * It also considers the predicted semantic types provided by the matcher.
    * NOTE: KarmaBuildAlignmentGraph needs to be executed before suggesting anything.
    *
    * @param ssd SemanticSourceDesc for which we need to learn the alignment.
    * @param ontologies List of location strings for ontologies of this ssd.
    * @param dsPredictions Matcher DataSetPrediction object which contains predictions for columns in the dataset.
    * @param semanticTypeMap Mapping of matcher:labels to URIs (just namespace actually).
    * @param attrToColMap Mapping of modeller:attributes to matcher:columns.
    * @param numSemanticTypes Integer which indicates how many top semantic types to keep; default is 4.
    * @return SSDPrediction wrapped into Option
    */
  def suggestModels(ssd: Ssd
                    , ontologies: List[String]
                    , dsPredictions: Option[DataSetPrediction]
                    , semanticTypeMap: Map[String, String]
                    , attrToColMap: Map[AttrID,ColumnID]
                    , numSemanticTypes: Int = defaultNumSemanticTypes
                   ): Option[SsdPrediction] = {
    karmaInitialize()
    Try {
      // we need to make sure that the specified ontologies in ssd are in Karma preloaded-ontologies directory
      if (!ontologies.map(Paths.get(_).getFileName.toString)
        .forall(karmaWrapper.ontologies.map(Paths.get(_).getFileName.toString).contains))
      {
        logger.error(s"Ontologies specified in SSD ${ssd.id} are not preloaded to Karma.")
        throw ModelerException(s"Ontologies specified in SSD ${ssd.id} are not preloaded to Karma.")
      }

      val initialAlignment = getAlignment(ssd)
      // if we have predictions from the schema matcher, then we should add not mapped attributes to the alignment
      val alignmentGraph: Alignment = dsPredictions match {
        case Some(dsPreds) =>
          setAlignmentSourceColumns(
            getKarmaNotMappedAttributes(ssd, dsPreds, semanticTypeMap, attrToColMap)
            , initialAlignment)
        case None =>
          initialAlignment //in case there are no predictions, we use the initial alignment
      }

      // debugging stuff
//      GraphUtil.exportJson(alignmentGraph.getGraphBuilder.getGraph,
//        Paths.get(ModelerConfig.KarmaDir, s"karma_learn_alignment_graph.json").toString, true, true)
//      logger.debug(s"---Alignment3 sourcecolnodes: ${alignmentGraph.getSourceColumnNodes.size}")

      val suggestions: List[(Ssd, SemanticScores)] =
        getKarmaSuggestions(ssd, alignmentGraph, numSemanticTypes)

      if(suggestions.nonEmpty) {
          logger.info(s"${suggestions.size} Suggestions for SSD ${ssd.id} successfully constructed.")
          Some(SsdPrediction(ssd.id, suggestions))
      } else {
          logger.info(s"No suggestions for SSD ${ssd.id} have been made.")
          None
      }
    } match {
      case Success(res) =>
        res
      case Failure(err) =>
        logger.warn(s"Failed to suggest semantic models for SSD ${ssd.id} with error: $err")
        None
    }
  }
}