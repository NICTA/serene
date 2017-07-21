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
package au.csiro.data61.gradoop

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.apache.flink.api.common.functions.{MapFunction, ReduceFunction}
import org.apache.flink.api.java.{DataSet => JavaDS, ExecutionEnvironment => JavaEnv}
import org.apache.flink.configuration.{Configuration => FlinkConfig}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileUtil, FileSystem => HdfsFS, Path => HdfsPath}
import org.gradoop.examples.dimspan.data_source.DIMSpanTLFSource
import org.gradoop.common.model.impl.{pojo, properties}
import org.gradoop.flink.algorithms.fsm.dimspan.functions.conversion.EPGMGraphTransactionToLabeledGraph
import org.gradoop.flink.algorithms.fsm.dimspan.tuples.LabeledGraphStringString
import org.gradoop.flink.algorithms.fsm.dimspan.DIMSpan
import org.gradoop.flink.io.impl.tlf.{TLFDataSink, TLFDataSource}
import org.gradoop.flink.algorithms.fsm.dimspan.config.DIMSpanConfig
import org.gradoop.flink.io.api.DataSink
import org.gradoop.flink.io.impl.json.JSONDataSink
import org.gradoop.flink.model.impl.{GraphCollection, GraphTransactions, LogicalGraph}
import org.gradoop.flink.representation.transactional.GraphTransaction
import org.gradoop.flink.util.GradoopFlinkConfig
import org.gradoop.common.model.impl.pojo.{Edge, GraphHead, Vertex}
import org.apache.flink.api.scala._
import org.gradoop.common.model.impl.id.{GradoopId, GradoopIdList}
import org.gradoop.common.model.impl.properties.{Properties, PropertyValue}
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.ExplorativePatternMatching
import au.csiro.data61.types.GraphTypes.NodeID
import au.csiro.data61.gradoop.NodeType.NodeType
import au.csiro.data61.types.{HelperLink, KarmaTypes, SemanticModel, SsdLabel, SsdNode}

import scala.collection.JavaConverters._
import scala.util.{Failure, Random, Success, Try}
import com.typesafe.scalalogging.LazyLogging

import scala.collection.AbstractSeq


/**
  * Some configuration for HDFS
  */
trait Hdfs {
  val HdfsDirName = "/dimspan"
  val HdfsDefault = "hdfs://localhost:9000"

  // set up hdfs
  val hdfsConfig = new Configuration()
  hdfsConfig.set("fs.defaultFS", HdfsDefault)
  val dfs = HdfsFS.get(hdfsConfig)
  // create temporary directory in HDFS
  val src = new HdfsPath(dfs.getWorkingDirectory + HdfsDirName)
  dfs.mkdirs(src)

  dfs.deleteOnExit(src)
}

/**
  * Case class to convert between our semantic model
  * and gradoop's DataSource for frequent graph pattern mining procedure DIMSpan
  * as well as for graph pattern matching.
  * There's currently a problem with the length of labels used for nodes and links.
  * @param semanticModels list of semantic models from which patterns will be mined
  * @param randomLabel boolean whether labels should be randomly reindexed; default is false
  */
case class DIMSpanTLFSourceWrapper(semanticModels: List[SemanticModel],
                                   randomLabel: Boolean = false) extends LazyLogging with Hdfs {

  private val FlinkLimit = 20
  private val LabelKeySize = 1
  private lazy val DataGraphPath = "/tmp/gradoop/data_graph.tlf"

  /**
    * Configuration to execute stuff on Flink
    */
  private val GRADOOP_CONFIG: GradoopFlinkConfig = {
    val config = new FlinkConfig()
    config.setInteger("taskmanager.network.numberOfBuffers", 16000)
    config.setInteger("taskmanager.numberOfTaskSlots", 2)
    config.setInteger("taskmanager.heap.mb", 1024)
    config.setInteger("jobmanager.heap.mb", 512)

    val env = JavaEnv.createLocalEnvironment(config)
//    env.setParallelism(4)
    GradoopFlinkConfig
      .createConfig(env)
  }

  // lookup Gradoop Id for each semantic model
  lazy val GradoopModelMap: Map[Int, GradoopId] =
    semanticModels
      .zipWithIndex
      .map { case (x: SemanticModel, y: Int) => (y, GradoopId.get()) }.toMap

  // lookup semantic models which contain node label
  lazy val NodeModelMap: Map[String, List[GradoopId]] = {
    semanticModels
      .zipWithIndex
      .flatMap {
        case (sm: SemanticModel, idx: Int) =>
          sm.getNodeLabelIds.map { s => (s, GradoopModelMap(idx))}
      }.groupBy(_._1)
      .map {
        case (group, trav) =>
          (group, trav.foldLeft(List[GradoopId]())(_ :+ _._2))
      }
  }

  // lookup semantic models which contain link label
  lazy val LinkModelMap: Map[String, List[GradoopId]] = {
    semanticModels
      .zipWithIndex
      .flatMap { case (sm: SemanticModel, idx:Int) =>
        sm.getLinkLabelIds.map { s => (s, GradoopModelMap(idx))}
      }.groupBy(_._1)
      .map {
        case (group, trav) =>
          (group, trav.foldLeft(List[GradoopId]())(_ :+ _._2))
      }
  }

  // indexing node labels in semantic models for creation of Gradoop structures
  lazy val GradoopNodeMap: Map[String, GradoopId] = {
    // for each distinct node label we have a distinct Gradoop Id
    semanticModels
      .flatMap(_.getNodeLabelIds).distinct
      .map( s => (s, GradoopId.get())).toMap
  }

  // indexing link labels in semantic models for creation of Gradoop structures
  lazy val GradoopLinkMap: Map[String, GradoopId] = {
    // for each distinct link label we have a distinct Gradoop Id
    semanticModels
      .flatMap(_.getLinkLabelIds).distinct
      .map( s => (s, GradoopId.get())).toMap
  }

  // indexing semantic models for further lookup
  lazy val TLFLookup: Map[Int, SemanticModel] = semanticModels
    .zipWithIndex
    .map { case (x:SemanticModel, y:Int) => (y,x) }.toMap

  // re-indexing labels since Gradoop cannot handle long labels
  lazy val LabelLookup: Map[String, String] = {
    val presentLabs = semanticModels
      .flatMap {
        semModel =>
          val nodeLabels: List[String] = semModel.getNodeLabels
            .map { n =>
              n.labelType match {
                case "DataNode" => n.label
                case _ => n.getURI
              }
            }
          val linkLabels: List[String] = semModel.getHelperLinks.map {
            link => s"${link.prefix}${link.label}"
          }
          (nodeLabels ::: linkLabels).distinct
      }.distinct

    if (randomLabel) {
      presentLabs.map {
        label => (label, Random.alphanumeric.take(LabelKeySize).mkString)
      }.toMap
    } else {
      val chars = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')
      presentLabs.zip(chars).map { case (a,b) => (a, b.toString)}.toMap
    }
  }

  /**
    * Get converted label for the given key.
    * If this key is not in LabelLookup, a question mark will be returned.
    * @param s string to lookup
    * @return
    */
  private def getLabel(s: String): String = {
    LabelLookup.getOrElse(s, "?")
  }

  /**
    * Get a new label for a label from the semantic model.
    * If relabeling is not needed, uri of the original label will be returned.
    * Otherwise, the shortened re-indexed version.
    * @param ssdLabel SsdLabel of a node or link from the semantic model
    * @param reLabel Boolean whether to relabel the original label
    * @return
    */
  private def getSmLabel(ssdLabel: SsdLabel, reLabel: Boolean): String = {
    if (reLabel) {
      ssdLabel.labelType match {
        case "DataNode" => getLabel(ssdLabel.label)
        case _ => getLabel(ssdLabel.getURI)
      }
    } else {ssdLabel.getURI}
  }

  /**
    * Construct NodeInfo for a given node.
    * This is used for filtering some nodes and links in the Gradoop conversion process.
    * @param ssdLabel SsdLabel of a node from the semantic model
    * @return
    */
  private def getNodeType(ssdLabel: SsdLabel): NodeType.NodeType = {
    ssdLabel.labelType match {
      case "DataNode" => NodeType.Data
      case _ => ssdLabel.getURI match {
        case SpecialNodes.UnknownCn => NodeType.Unknown
        case SpecialNodes.AllCn => NodeType.All
        case _ => NodeType.Normal
      }
    }
  }

  /**
    * Convert semantic model to a TLF string
      t # 0
      v 0 A
      v 1 B
      e 0 1 a
      e 0 1 b
    * @param semModel SemanticModel
    * @param reLabel Boolean to relabel node and link labels to make them shorter!
    * @return
    */
  protected def semanticModelToTLF(semModel: SemanticModel, reLabel: Boolean = true): String = {
    // Gradoop format TLF has problems with long integers!!!
    val nodeIdMap: Map[Int, Int] = semModel.getNodes.map(_.id).sorted.zipWithIndex.toMap

    // convert nodes to the TLF representation
    val nodeString:String = semModel.getNodes
      .sortBy(n => n.id)
      .map { n: SsdNode =>
        val label = if (reLabel) {
          n.ssdLabel.labelType match {
            case "DataNode" => getLabel(n.ssdLabel.label)
            case _ => getLabel(n.ssdLabel.getURI)
          }
        } else {n.ssdLabel.getURI}
        s"v ${nodeIdMap(n.id)} $label"
      }.mkString("\n")

    // convert links to the TLF representation
    val linkString: String = semModel.getHelperLinks
      .sortBy(e => (e.source, e.target, e.label))
      .map {link: HelperLink =>
        val label = if (reLabel) {getLabel(s"${link.prefix}${link.label}")} else {s"${link.prefix}${link.label}"}
        s"e ${nodeIdMap(link.source)} ${nodeIdMap(link.target)} $label"
      }.mkString("\n")

    nodeString + "\n" + linkString
  }

  /**
    * Write semantic models to a file in TLF format.
    * Note: we need to sort vertices and edges!!!
    * @param reLabel Boolean to relabel node and link labels to make them shorter!
    * @param outputPath Optional string where the output will be written
    */
  def toTLF(reLabel: Boolean = true, outputPath: Option[String] = None): Try[String] = {
    logger.info(s"Converting ${semanticModels.size} semantic models to TLF format...")
    val outputString: String =
    TLFLookup.map {
      case (idx: Int, semModel: SemanticModel) =>
        s"t # $idx\n" + semanticModelToTLF(semModel, reLabel)
    }.mkString("\n")

    Try {

      outputPath.map {
        strPath: String =>
          logger.info(s"Writing TLF file for semantic models to $strPath")
          val out = Paths.get(strPath)
          if (!out.toFile.getParentFile.exists) {
            out.toFile.getParentFile.mkdirs
          }

          Files.write(
            out,
            outputString.getBytes(StandardCharsets.UTF_8)
          )
      }
      outputString
    }
  }

  /**
    * Merge partitions from HDFS into one local file
    * @param hdfsName Name of directory to be merged
    * @param localName Local file path
    * @param dfs Hadoop file system
    * @param hdfsConfig configuration for Hadoop
    */
  def hdfsMergeFiles(hdfsName: String,
                     localName: String,
                     dfs: HdfsFS,
                     hdfsConfig: Configuration): Try[String] = {
    logger.info("Merging hdfs files to local filesystem")
    val localFS = HdfsFS.getLocal(hdfsConfig)
    val returnName = Try {
      if (hdfsName.toLowerCase.endsWith(".json")) {
        // merge folder for edges
        FileUtil.copyMerge(dfs, new HdfsPath(hdfsName + "/edges.json"),
          localFS, new HdfsPath(localName + "/edges.json"), true, hdfsConfig, null)
        // merge folder for vertices
        FileUtil.copyMerge(dfs, new HdfsPath(hdfsName + "/vertices.json"),
          localFS, new HdfsPath(localName + "/vertices.json"), true, hdfsConfig, null)
        // merge folder for graphs
        FileUtil.copyMerge(dfs, new HdfsPath(hdfsName + "/graphs.json"),
          localFS, new HdfsPath(localName + "/graphs.json"), true, hdfsConfig, null)

      } else {
        FileUtil.copyMerge(dfs, new HdfsPath(hdfsName), localFS, new HdfsPath(localName), false, hdfsConfig, null)
      }
      localName
    } match {
      case Success(s) =>
        logger.debug(s"Files successfully merged to the local file system: $s")
        Success(s)
      case Failure(err) =>
        logger.error(s"Merging to the local file system failed: ${err.getMessage}")
        Failure(err)
    }
    localFS.close()
    returnName
  }


  /**
    * Helper method to execute only frequent graph pattern mining.
    * @param inputPath name of file where convertd tlf representation will be written.
    * @param minSupport threshold for support; this is a value in range [0,1]
    * @param directed boolean whether graphs are considered directed or not
    * @return
    */
  private def getFreqPatterns(inputPath: String,
                              minSupport: Double,
                              directed: Boolean): JavaDS[GraphTransaction] = {
    // copy tlf with semantic models to HDFS
    val inName = dfs.getWorkingDirectory + s"$HdfsDirName/" + Paths.get(inputPath).getFileName.toString
    logger.info(s"Copying tlf file to HDFS: $inName")
    Try {
      dfs.copyFromLocalFile(new HdfsPath(inputPath), new HdfsPath(inName))
    } match {
      case Success(_) => logger.info("File copied successfull.")
      case Failure(err) =>
        logger.error(s"Failed to copy the file to HDFS: ${err.getMessage}")
    }

    // Create data source and sink
    val dataSource: DIMSpanTLFSource = new DIMSpanTLFSource(inName, GRADOOP_CONFIG)

    logger.info("Initializing miner")
    val fsmConfig: DIMSpanConfig = new DIMSpanConfig(minSupport.asInstanceOf[Float], directed)

    // Change default configuration here using setter methods
    new DIMSpan(fsmConfig)
      .execute(dataSource.getGraphs)
  }

  private def filterNode(nodeInfo: NodeInfo,
                         skipData: Boolean = false,
                         skipUnknown: Boolean = false): Boolean = {
    !(skipData & nodeInfo.nodeType == NodeType.Data) &
      !(skipUnknown & nodeInfo.nodeType == NodeType.Unknown) &
      !(skipUnknown & nodeInfo.nodeType == NodeType.All)
  }

  private def getLgNodes(reLabel: Boolean = true,
                         skipData: Boolean = false,
                         skipUnknown: Boolean = false): (Map[String, NodeInfo], List[Vertex]) = {
    logger.debug("Converting nodes from semantic models to Logical Graph Vertex")
    // lookup table from extended node label id to a normal label
    val labelMap: Map[String, NodeInfo] = semanticModels.flatMap {
      sm: SemanticModel => sm.getNodes.map {
        n =>
          val label = getSmLabel(n.ssdLabel, reLabel)
          val nodeType: NodeType.NodeType = getNodeType(n.ssdLabel)
          (sm.getExtendedLabel(n), NodeInfo(label, nodeType))
      }
    }.distinct.toMap

    val nodes: List[Vertex] = NodeModelMap.filterKeys {
      nlId: String => filterNode(labelMap(nlId), skipData, skipUnknown)
    }.map {
      case (nlId, modelIds) =>
        new Vertex(GradoopNodeMap(nlId),
          labelMap(nlId).label,
          new Properties(), // no properties for patterns
          GradoopIdList.fromExisting(modelIds.asJava))
    }.toList

    (labelMap, nodes)
  }


  def getLgLinks(labelMap: Map[String, NodeInfo],
                 reLabel: Boolean = true,
                 skipData: Boolean = false,
                 skipUnknown: Boolean = false): List[Edge] = {
    logger.debug("Converting links from semantic models to Logical Graph Edge")
    // map from link extended label to label
    val eLabelMap: Map[String, String] = semanticModels.flatMap {
      sm: SemanticModel => sm.getLinks.map {
        e =>
          val label = getSmLabel(e.ssdLabel, reLabel)
          (sm.getExtendedLabel(e.from, e.ssdLabel.getURI, e.to), label)
      }
    }.distinct.toMap

    val links: List[Edge] = LinkModelMap.filterKeys {
      elId: String =>
        // strings = (source_label: String, eL: String, target_label: String)
        val strings = elId.split("---")
        filterNode(labelMap(strings(0)), skipData, skipUnknown) &
          filterNode(labelMap(strings(2)), skipData, skipUnknown)
    }.map {
      case (elId, modelIds) =>
        // strings = (source_label: String, eL: String, target_label: String)
        val strings = elId.split("---")
        val (fromId: GradoopId, toId: GradoopId) = (GradoopNodeMap(strings(0)), GradoopNodeMap(strings(2)))

        new Edge(GradoopLinkMap(elId),
          eLabelMap(elId),
          fromId,
          toId,
          new Properties(),
          GradoopIdList.fromExisting(modelIds.asJava)
        )
    }.toList

    links
  }

  /**
    * Convert Semantic Models to Gradoop's Logical Graph.
    * If skipData is set to true, then data nodes and data properties will not be added to the Logical Graph.
    * If skipUnknown is set to true, then class nodes for Unknown and All will not be added to the Logical Graph
    * as well as object properties which connect them.
    * @param reLabel Boolean whether to reindex labels (to make them shorter!)
    * @param skipData Boolean to skip data nodes and data property links
    * @param skipUnknown Boolean to skip class nodes and object property links related to All and Unknown
    * @return
    */
  def smToLogicalGraph(reLabel: Boolean = true,
                       skipData: Boolean = false,
                       skipUnknown: Boolean = false): Try[LogicalGraph] = Try {
    logger.info("Converting semantic models to the logical graph of Gradoop")

    val (labelMap: Map[String, NodeInfo], nodes: List[Vertex]) = getLgNodes(reLabel, skipData, skipUnknown)
    logger.info("Nodes have been converted.")

    val links: List[Edge] = getLgLinks(labelMap, reLabel, skipData, skipUnknown)

    logger.info("Links have been converted.")

    val graphHeads: List[GraphHead] = GradoopModelMap.values.map {
      idx => new GraphHead(idx, "semanticModel", new Properties())
    }.toList

    logger.info("GraphHeads have been created.")

    val lg = LogicalGraph.fromDataSets(
      GRADOOP_CONFIG.getExecutionEnvironment.fromCollection(graphHeads.asJavaCollection),
      GRADOOP_CONFIG.getExecutionEnvironment.fromCollection(nodes.asJavaCollection),
      GRADOOP_CONFIG.getExecutionEnvironment.fromCollection(links.asJavaCollection),
      GRADOOP_CONFIG
    )
    // for debugging purposes
    val dataSink: DataSink = new JSONDataSink(Paths.get("/tmp", "gradoop", "smloggraph.json").toString,
      GRADOOP_CONFIG)
    dataSink.write(lg, true)

    lg
  }

  /**
    * Method to convert semantic models to Gradoop's GraphCollection
    * @param skipData Boolean to skip data nodes and data property links
    * @param skipUnknown Boolean to skip class nodes and object property links related to All and Unknown
    * @return
    */
  def getGraphCollection(skipData: Boolean = false,
                         skipUnknown: Boolean = false): Try[GraphCollection] = Try {
    logger.debug("Converting semantic models to GraphCollection")
    // converting our semantic models to Gradoop's data structures
    val collection: GraphCollection = smToLogicalGraph(reLabel = true, skipData, skipUnknown) match {
      case Success(gr: LogicalGraph) =>
        logger.info("Creating graph collection from the converted logical graph")
        GraphCollection.fromGraph(gr)
      case Failure(err) =>
        val mess = s"Convertion of semantic models to Gradoop LogicalGraph failed: ${err.getMessage}"
        logger.error(mess)
        throw new Exception(mess)
    }

    // for debugging purposes write the collection to disk
    val dataSink: DataSink = new JSONDataSink(Paths.get("/tmp","gradoop","collection").toString,
      GRADOOP_CONFIG)
    dataSink.write(collection, true)

    collection
  }

  /**
    * Helper method to execute only frequent graph pattern mining.
    * @param minSupport threshold for support; this is a value in range [0,1]
    * @param directed boolean whether graphs are considered directed or not
    * @param skipData Boolean to skip data nodes and data property links
    * @param skipUnknown Boolean to skip class nodes and object property links related to All and Unknown
    * @return
    */
  private def getFreqPatterns(minSupport: Double,
                              directed: Boolean,
                              skipData: Boolean,
                              skipUnknown: Boolean): JavaDS[GraphTransaction] =  {

    // convert Gradoop graph collection to DIMSpan input format
    val input: JavaDS[LabeledGraphStringString] = getGraphCollection(skipData, skipUnknown) match {
      case Success(gr: GraphCollection) =>
        gr.toTransactions
          .getTransactions.map(new EPGMGraphTransactionToLabeledGraph())
      case Failure(err) =>
        logger.error(s"Failed to get graph collection: ${err.getMessage}")
        throw new Exception(s"Failed to get graph collection: ${err.getMessage}")
    }

    logger.info("Initializing miner")
    val fsmConfig: DIMSpanConfig = new DIMSpanConfig(minSupport.asInstanceOf[Float], directed)

    // Change default configuration here using setter methods
    new DIMSpan(fsmConfig)
      .execute(input)
  }

  /**
    * Method to execute frequent graph pattern mining and to convert patterns to GDL.
    * @param inputPath name of file where convertd tlf representation will be written.
    * @param outputPath name of file to write the discovered patterns
    * @param minSupport threshold for support; this is a value in range [0,1]
    * @param directed boolean whether graphs are considered directed or not
    * @return
    */
  def executeMiningGDL(inputPath: String,
                       outputPath: String,
                       minSupport: Double,
                       directed: Boolean): Try[DataSet[GDLPattern]] = Try {
    // Create data sink
    val outName = dfs.getWorkingDirectory + s"$HdfsDirName/" + Paths.get(outputPath).getFileName.toString
    val dataSink: DataSink = if (outputPath.toLowerCase.endsWith(".json")) {
      logger.info("Initializing json data sink")
      // JSONDataSink also outputs support measures for patterns unlike TLFDataSink
      new JSONDataSink(outName, GRADOOP_CONFIG)
    } else {
      logger.info("Initializing default tlf data sink")
      new TLFDataSink(outName, GRADOOP_CONFIG)
    }

    // Change default configuration here using setter methods
    val frequentPatterns: JavaDS[GraphTransaction] = getFreqPatterns(
      inputPath, minSupport, directed)

    // write as GDL
    val gdlPats: DataSet[GDLPattern] = new DataSet[GraphTransaction](frequentPatterns) // converting to scala DataSet
      .map(new graphToGDLString)

    // Execute and write to disk
    dataSink.write(new GraphTransactions(frequentPatterns, GRADOOP_CONFIG), true)
    GRADOOP_CONFIG.getExecutionEnvironment.execute
    logger.info("Pattern mining finished")
    gdlPats
  }

  /**
    * Method to execute frequent graph pattern mining and to convert patterns to GDL.
    * @param outputPath name of file to write the discovered patterns
    * @param minSupport threshold for support; this is a value in range [0,1]
    * @param directed boolean whether graphs are considered directed or not
    * @param skipData Boolean to skip data nodes and data property links
    * @param skipUnknown Boolean to skip class nodes and object property links related to All and Unknown
    * @return
    */
  def mineGDL(outputPath: String,
              minSupport: Double,
              directed: Boolean,
              skipData: Boolean = false,
              skipUnknown: Boolean = false): Try[DataSet[GDLPattern]] = Try {
    // Create data sink
    val dataSink: DataSink = if (outputPath.toLowerCase.endsWith(".json")) {
      logger.info("Initializing json data sink")
      // JSONDataSink also outputs support measures for patterns unlike TLFDataSink
      new JSONDataSink(outputPath, GRADOOP_CONFIG)
    } else {
      logger.info("Initializing default tlf data sink")
      new TLFDataSink(outputPath, GRADOOP_CONFIG)
    }

    // Change default configuration here using setter methods
    val frequentPatterns: JavaDS[GraphTransaction] = Try {
      getFreqPatterns(minSupport, directed, skipData, skipUnknown)
    } match {
      case Success(pats) =>
        logger.info(s"Obtained graph patterns: ${pats.count}")
        pats
      case Failure(err) =>
        logger.error(s"Failed to get patterns: ${err.getMessage}")
        throw new Exception(s"Failed to get patterns: ${err.getMessage}")
    }


    // write as GDL
    val gdlPats: DataSet[GDLPattern] = new DataSet[GraphTransaction](frequentPatterns) // converting to scala DataSet
      .map(new graphToGDLString)

    // Execute and write to disk
    dataSink.write(new GraphTransactions(frequentPatterns, GRADOOP_CONFIG), true)
    GRADOOP_CONFIG.getExecutionEnvironment.execute
    logger.info("Pattern mining finished")
    gdlPats
  }

  /**
    * Helper method to execute frequent graph pattern mining and to save patterns to file.
    * @param inputPath name of file where convertd tlf representation will be written.
    * @param outputPath name of file to write the discovered patterns
    * @param minSupport threshold for support; this is a value in range [0,1]
    * @param directed boolean whether graphs are considered directed or not
    * @return
    */
  private def executeMiningSave(inputPath: String,
                                outputPath: String,
                                minSupport: Double,
                                directed: Boolean): Try[String] = Try {
    // Create data sink
    val outName = dfs.getWorkingDirectory + s"$HdfsDirName/" + Paths.get(outputPath).getFileName.toString
    val dataSink: DataSink = if (outputPath.toLowerCase.endsWith(".json")) {
      logger.info("Initializing json data sink")
      // JSONDataSink also outputs support measures for patterns unlike TLFDataSink
      new JSONDataSink(outName, GRADOOP_CONFIG)
    } else {
      logger.info("Initializing default tlf data sink")
      new TLFDataSink(outName, GRADOOP_CONFIG)
    }

    // Change default configuration here using setter methods
    val frequentPatterns: JavaDS[GraphTransaction] = getFreqPatterns(
      inputPath, minSupport, directed)

    // Execute and write to disk
    dataSink.write(new GraphTransactions(frequentPatterns, GRADOOP_CONFIG), true)
    GRADOOP_CONFIG.getExecutionEnvironment.execute
    logger.info("Pattern mining finished")
    outName
  }

  /**
    * Mine frequent patterns with support above threshold minSupport in the set of semantic models.
    * This set of semantic models are treated as a database of graphs.
    * Gradoop procedure DIMSpan is used for mining.
    * reLabel should be set to true for now, Gradoop cannot handle long labels.
    * Frequent patterns are saved to file.
    * @param inputPath name of file where converted tlf representation will be written.
    * @param outputPath name of file to write the discovered patterns
    * @param minSupport threshold for support; this is a value in range [0,1]
    * @param directed boolean whether graphs are considered directed or not
    * @param reLabel boolean whether node and link labels should be reindexed.
    */
  def saveFrequentPatterns(inputPath: String,
                           outputPath: String,
                           minSupport: Double,
                           directed: Boolean,
                           reLabel: Boolean = true): Option[String] = {
    // convert to TLF
    val tlfString = toTLF(reLabel, Some(inputPath))

    val result = (for {
      // execute graph pattern mining with Gradoop
      outName <- executeMiningSave(inputPath, outputPath, minSupport, directed)
      // get result from HDFS to the local file system
      resultPath <- hdfsMergeFiles(outName, outputPath, dfs, hdfsConfig)
    } yield resultPath ) match {
      case Success(s) =>
        logger.info("Frequent pattern mining successfully finished.")
        Some(s)
    case Failure(err) =>
      logger.error(s"Frequent pattern mining failed: ${err.getMessage}")
      None
    }

    // delete temporary directory from HDFS & close
    dfs.deleteOnExit(src)
    dfs.close()

    result
  }

  /**
    * Convert Karma alignment graph to tlf format and save to a temporary file.
    * Put this tlf file to HDFS.
    * @param alignGraphJson location of the alignment graph
    * @return
    */
  def putDataGraph(alignGraphJson: String): Try[String] = Try {
    val (dataGraph: SemanticModel, _, _) = KarmaTypes.readAlignmentGraph(alignGraphJson)
    // convert to TLF
    val tlfString = "t # 0\n" + semanticModelToTLF(dataGraph, reLabel = true)
    logger.info(s"Writing TLF file for the data graph")
    val out = Paths.get(DataGraphPath)
    if (!out.toFile.getParentFile.exists) {
      out.toFile.getParentFile.mkdirs
    }
    Files.write(
      out,
      tlfString.getBytes(StandardCharsets.UTF_8)
    )

    // copy tlf to HDFS
    val inName = dfs.getWorkingDirectory + s"$HdfsDirName/" + out.getFileName.toString
    logger.info(s"Copying tlf file to HDFS: $inName")
    dfs.copyFromLocalFile(new HdfsPath(out.toString), new HdfsPath(inName))

    inName
  }

  /**
    * Create instance of Gradoop's Properties either for a node or for a link
    * @param key integer number
    * @param keyType type of the node/link which key represents
    * @param origLabel original label string
    * @param origIdMap map for original ids
    * @param createProps boolean whether to fill in properties with something
    * @return
    */
  private def createProperties(key: Int,
                               keyType: String,
                               origLabel: String,
                               origIdMap: Option[Map[Int, String]],
                               createProps: Boolean = true
                              ): Properties = {
    val props: Properties = new Properties()

    if (createProps) {
      props.set("originalId", PropertyValue.create(key))
      props.set("originalLabel", PropertyValue.create(origLabel))
      props.set("originalType", PropertyValue.create(keyType))
      if (origIdMap.isDefined) {
        props.set("alignId", PropertyValue.create(origIdMap.get(key)))
      }
    }

    props
  }


  private def createEdge(dataGraph: SemanticModel,
                         linkId: Int,
                         linkLabel: SsdLabel,
                         source: SsdNode,
                         target: SsdNode,
                         graphHead: GraphHead,
                         origLinkIdMap: Option[Map[Int, String]],
                         nodeIdMap: Map[Int, GradoopId],
                         reLabel: Boolean = true,
                         createProps: Boolean = true
                        ): Edge = {
    val label = if (reLabel) {
      getLabel(linkLabel.getURI)
    } else {linkLabel.getURI}

    val (lId, fromId, toId) = if (createProps) {
      (GradoopId.get(), nodeIdMap(source.id), nodeIdMap(target.id))
    } else {
      (GradoopLinkMap(dataGraph.getExtendedLabel(source, linkLabel.getURI, target)),
        GradoopNodeMap(dataGraph.getExtendedLabel(source)),
        GradoopNodeMap(dataGraph.getExtendedLabel(target))
        )
    }

    new Edge(lId,
      label,
      fromId,
      toId,
      createProperties(linkId, linkLabel.labelType, linkLabel.getURI, origLinkIdMap, createProps),
      GradoopIdList.fromExisting(graphHead.getId)
    )
  }

  /**
    * Convert Semantic Model to Gradoop's Logical Graph.
    * @param dataGraph Semantic Model
    * @param origNodeIdMap Map for original node ids
    * @param origLinkIdMap Map for original link ids
    * @param reLabel Boolean whether to reindex labels (to make them shorter!)
    * @param createProps boolean whether to fill in properties with something
    * @return
    */
  def getLogicalGraph(dataGraph: SemanticModel,
                      origNodeIdMap: Option[Map[Int, String]],
                      origLinkIdMap: Option[Map[Int, String]],
                      reLabel: Boolean = true,
                      createProps: Boolean = true): Try[LogicalGraph] = Try {
    logger.info("Converting a semantic model to logical graph of Gradoop")
    val id = GradoopId.get()
    val graphHead: GraphHead = new GraphHead(id, "dataGraph", new Properties())

    val nodeIdMap: Map[NodeID, GradoopId] = dataGraph.getNodes.map {
      n => (n.id, GradoopId.get())
    }.toMap

    val nodes: List[Vertex] = dataGraph.getNodes.map {
      n =>
        val label = if (reLabel) {
          n.ssdLabel.labelType match {
            case "DataNode" => getLabel(n.ssdLabel.label)
            case _ => getLabel(n.ssdLabel.getURI)
          }
        } else {n.ssdLabel.getURI}
        val nId = if (createProps) {nodeIdMap(n.id)} else {GradoopNodeMap(dataGraph.getExtendedLabel(n))}
        new Vertex(nId,
          label,
          createProperties(n.id, n.ssdLabel.labelType, n.ssdLabel.getURI, origNodeIdMap, createProps),
          GradoopIdList.fromExisting(graphHead.getId)
        )
    }

    logger.info("Nodes have been converted.")

    val links: List[Edge] = dataGraph.getLinks.map {
      link => createEdge(dataGraph, link.id, link.ssdLabel, link.from, link.to, graphHead,
        origLinkIdMap, nodeIdMap, reLabel, createProps)
    }

    logger.info("Links have been converted.")

    val lg = LogicalGraph.fromCollections(graphHead, nodes.asJavaCollection, links.asJavaCollection, GRADOOP_CONFIG)
    // for debugging purposes

    val dataSink: DataSink = new JSONDataSink(Paths.get("/tmp","gradoop",s"lg_${id}.json").toString,
      GRADOOP_CONFIG)
    dataSink.write(lg, true)

    lg
  }

  /**
    * Query 1 pattern in the data graph.
    * Wrapping it into Try makes no sense since execution is not performed here.
    * @param graph Gradoop's logical graph where pattern will be searched
    * @param query GDLPattern which contains query pattern as GDL string and its support
    * @return
    */
  def queryPattern(graph: LogicalGraph,
                   query: GDLPattern): Option[GraphCollection] = {
    logger.info(s"Getting embeddings for pattern ${query.gdlQuery}")
    lazy val result = new ExplorativePatternMatching.Builder()
      .setAttachData(true)
      .setQuery(query.gdlQuery)
      .setMatchStrategy(MatchStrategy.ISOMORPHISM)
      .build()
      .execute(graph)

    // for further operations we need to make sure that the result is not empty
    if (result.isEmpty.collect().asScala.reduce(_&_)) {
      logger.info(s"No embeddings found for query ${query.gdlQuery}")
      None
    } else {
      logger.info(s"Embeddings found successfully for query ${query.gdlQuery}")
      // we want to store supports as part of found embeddings
      lazy val newGraphHeads: JavaDS[GraphHead] = result.getGraphHeads
        .map(new addSupportGraphHead().setVal(query.support))

      Some(GraphCollection.fromDataSets(newGraphHeads,
        result.getVertices,
        result.getEdges,
        GRADOOP_CONFIG))
    }
  }


  /**
    *
    * @param alignGraphJson json file which contains the alignment graph
    * @param gdlPatterns DataSet of patterns to query in the alignment graph
    * @param linkSize Range of sizes for patterns for which embeddings will be searched
    */
  def findEmbeddings(alignGraphJson: String,
                     gdlPatterns: DataSet[GDLPattern],
                     linkSize: SizeRange = SizeRange(),
                     writeFile: String): GraphCollection = {

    val (alignGraph: SemanticModel, nodeIdMap: Map[String, Int], linkIdMap: Map[String, Int]) =
      KarmaTypes.readAlignmentGraph(alignGraphJson)
    val graph: LogicalGraph = getLogicalGraph(alignGraph,
      Some(nodeIdMap.map { case (a,b) => (b, a)}.toMap),
      Some(linkIdMap.map { case (a,b) => (b, a)}.toMap)) match {
      case Success(lg) =>
        logger.info("Logical graph extracted successfully from the alignment graph")
        lg
      case Failure(err) =>
        logger.error(s"Failed to convert the alignment graph to logical graph: ${err.getMessage}")
        throw new Exception(s"Failed to convert the alignment graph to logical graph: ${err.getMessage}")
    }

    // for debugging purposes write alignment graph to disk
    val dataSink: DataSink = new TLFDataSink(Paths.get("/tmp","gradoop","alignment.tlf").toString,
      GRADOOP_CONFIG)
    dataSink.write(graph, true)

    def patSize(pat: GDLPattern): Boolean = {
      if (linkSize.lower.isDefined) {
        if (linkSize.upper.isDefined) {
          pat.linkSize > linkSize.lower.get & pat.linkSize < linkSize.upper.get
        } else {pat.linkSize > linkSize.lower.get}
      } else {
        if (linkSize.upper.isDefined) {
          pat.linkSize < linkSize.upper.get
        } else { true }
      }
    }

    // did not come up with a better way to perform graph pattern matching for all patterns
    // we query each pattern sequentially
    val selPats: List[GDLPattern] = gdlPatterns.collect()
      .filter(patSize).toList

    logger.info(s"Selected ${selPats.size} patterns to find embeddings")

    val listEmbeds: Seq[GraphCollection] = selPats
      .flatMap {
        pat => queryPattern(graph, pat)
      }

    logger.info(s"Embeddings have been found for ${listEmbeds.size} patterns")

    // Flink does not support union operators with more than 64 input data sets at the moment.

    val allEmbeds = recursiveEmbedding(graph, selPats)
//    val outName = dfs.getWorkingDirectory + s"$HdfsDirName/" + Paths.get("/tmp/gradoop/support_embeddings/").getFileName.toString
    val outName = Paths.get(writeFile).toString
    allEmbeds.writeTo(new JSONDataSink(outName, allEmbeds.getConfig))

    // execute
    logger.info("Starting execution for finding embeddings..")
    GRADOOP_CONFIG.getExecutionEnvironment.execute
    allEmbeds
  }

  def recursiveEmbedding(dataGraph: LogicalGraph,
                         patterns: List[GDLPattern],
                         groupId: Int = -1): GraphCollection = {

    logger.info(s"Mining ${patterns.size} patterns in data graph")
    if (patterns.size > FlinkLimit) {
      val listEmbeds: List[GraphCollection] = patterns
        .grouped(FlinkLimit)
        .zipWithIndex
        .map {
          case (patGroup, idx) => recursiveEmbedding(dataGraph, patGroup, idx)
        }.toList

      logger.info(s"--- Embeddings have been found for ${listEmbeds.size} groups")

      // Flink does not support union operators with more than 64 input data sets at the moment.
//      val allEmbeds = recursiveUnion(listEmbeds)
//      allEmbeds
//      listEmbeds.reduce(_.union(_))
//      val outName = Paths.get(s"/tmp/gradoop/temp_support_embeddings_idx$groupId/").toString
//      val dataSink = new JSONDataSink(outName, GRADOOP_CONFIG)
      val allEmbeds = listEmbeds.reduce(_.union(_))
//      dataSink.write(allEmbeds, true)
//      GRADOOP_CONFIG.getExecutionEnvironment.execute
      allEmbeds
    } else {
      val listEmbeds: Seq[GraphCollection] = patterns
        .flatMap {
          pat => queryPattern(dataGraph, pat)
        }
      logger.info(s"*** Embeddings have been found for ${listEmbeds.size} patterns")

      // Flink does not support union operators with more than 64 input data sets at the moment.
      val outName = Paths.get(s"/tmp/gradoop/temp_support_embeddings_idx$groupId/").toString
      val dataSink = new JSONDataSink(outName, GRADOOP_CONFIG)
//      val allEmbeds = recursiveUnion(listEmbeds)
      val emb = listEmbeds.reduce(_.union(_))
      dataSink.write(emb, true)
//      GRADOOP_CONFIG.getExecutionEnvironment.execute
      GraphCollection.fromCollections(emb.getGraphHeads.collect,
        emb.getVertices.collect, emb.getEdges.collect, emb.getConfig)
//      emb
    }
  }

  /**
    * Flink does not support union operators with more than 64 input data sets at the moment.
    * @param embeds Some Sequence of graph collection
    * @tparam T descendant of Abstract sequence of GraphCollection
    * @return
    */
  def recursiveUnion[T <: Iterable[GraphCollection]](embeds: T): GraphCollection = {
    val collection: List[GraphCollection] = if (embeds.size > FlinkLimit) {
      embeds.grouped(FlinkLimit).map(gr => recursiveUnion(gr)).toList
    } else {
      embeds.map {
        emb: GraphCollection =>
          GraphCollection.fromDataSets(emb.getGraphHeads, emb.getVertices, emb.getEdges, emb.getConfig)
      }.toList
    }

    logger.info(s"Recursive union for collection: ${collection.size}")

    if (collection.size > 1) {
      logger.info("reducing collection")
      val reduced = collection.reduce(_.union(_))
//      val outName = dfs.getWorkingDirectory + s"$HdfsDirName/" + Paths.get("/tmp/gradoop/support_embeddings/").getFileName.toString
//      val dataSink = new JSONDataSink(outName, GRADOOP_CONFIG)
//      dataSink.write(reduced, true)
//      GRADOOP_CONFIG.getExecutionEnvironment.execute
      reduced
    } else {
      if (collection.size == 1) {
        logger.info("giving head of collection")
        collection.head
      }
      else {
        logger.info("giving empty collection")
        GraphCollection.createEmptyCollection(GRADOOP_CONFIG)
      }
    }
  }
}


/**
  * Mapper function to write a frequent pattern from gradoop as GDL -- cypher like representation.
  * The main reason for such transformation is to use frequent patterns as queries for pattern matching in
  * the alignment graph later on.
  * GDL example: (n1:n1_label)-[:link_label]->(n2:n2_label)
  */
class graphToGDLString extends MapFunction[GraphTransaction, GDLPattern] {
  def linksToGDLString(nodes: java.util.Set[Vertex],
                       links: java.util.Set[Edge]): String = {
    // get labels of nodes
    val nodeLabelMap: Map[GradoopId, String] = nodes.asScala
      .map { n => (n.getId, n.getLabel)}.toMap
    // change node ids to be simple numbers
    val nodeIdMap: Map[GradoopId, Int] = nodes.asScala.zipWithIndex
      .map { case (n, id) => (n.getId, id)}.toMap
    links.asScala.map { link =>
      s"(n${nodeIdMap(link.getSourceId)}:" + nodeLabelMap(link.getSourceId) +
        ")-[:" + link.getLabel + s"]->(n${nodeIdMap(link.getTargetId)}:" +
        nodeLabelMap(link.getTargetId) + ")"
    }.mkString(", ")
  }

  override def map(g: GraphTransaction): GDLPattern = {
    val support: Double = g.getGraphHead.getPropertyValue("support").getFloat
    val query = "g" +
      "[" + linksToGDLString(g.getVertices, g.getEdges) + "]"
    GDLPattern(support, query, g.getEdges.size)
  }
}


class addSupportGraphHead extends MapFunction[GraphHead, GraphHead] {
  var support = 1.0
  def setVal(value: Double): addSupportGraphHead = {
    support = value
    this
  }

  override def map(g: GraphHead): GraphHead = {
    g.setProperty("support", PropertyValue.create(support))
    g
  }
}


/**
  * Represent graph patterns in GDL format with support
  * @param support Double
  * @param gdlQuery String
  * @param linkSize Number of links
  */
case class GDLPattern(support: Double, gdlQuery: String, linkSize: Int)

/**
  * Enumeration for the node type which is needed for conversion of semantic models to Gradoop
  */
object NodeType extends Enumeration {
  type NodeType = Value

  val All = Value("all")
  val Unknown = Value("unknown")
  val Data = Value("data")
  val Normal = Value("normal")
}

/**
  * Node information which is needed for conversion of semantic models to Gradoop
  * @param label string which is used as a label for the node in Gradoop
  * @param nodeType node type
  */
case class NodeInfo(label: String, nodeType: NodeType)

/**
  * Some special nodes which are used in semantic models
  */
object SpecialNodes {
  val DefaultNs = "http://au.csiro.data61/serene/dev#"
  val UnknownCn = DefaultNs + "Unknown"  // label of the class node for the unknown class
  val AllCn = DefaultNs + "All"  // label of the class node which connects all other classes
}

/**
  * Simple class to represent a range of integers.
  * This is used to filter patterns within specific range.
  * @param lower Optional integer for the lower bound
  * @param upper Optional integer for the upper bount
  */
case class SizeRange(lower: Option[Int] = None,
                     upper: Option[Int] = None)