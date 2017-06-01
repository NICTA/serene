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

import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import java.nio.charset.StandardCharsets

import language.postfixOps
import scala.collection.JavaConverters._
import com.typesafe.scalalogging.LazyLogging
import edu.isi.karma.webserver._
import edu.isi.karma.config.{ModelingConfiguration, ModelingConfigurationRegistry}
import edu.isi.karma.rep.{Workspace, WorkspaceManager}
import edu.isi.karma.webserver.ServletContextParameterMap.ContextParameter
import edu.isi.karma.controller.update.UpdateContainer
import edu.isi.karma.metadata.OntologyMetadata
import edu.isi.karma.rep.metadata.Tag
import edu.isi.karma.rep.metadata.TagsContainer.{Color, TagName}
import edu.isi.karma.modeling.alignment.{SemanticModel => KarmaSsd}
import au.csiro.data61.types.{KarmaSemanticModel, TypeConfig}
import au.csiro.data61.types.Exceptions._
import au.csiro.data61.modeler.ModelerConfig

import scala.util.{ Try, Success, Failure}


/**
  * Class to initialize Karma tool.
  * @param alignmentDir Directory where the alignment graph is/will be stored
  * @param ontologies List of paths where ontologies are stored
  * @param modelingProps String which corresponds to Karma style of modeling properties; optional
  */
case class KarmaParams(alignmentDir: String
                       , ontologies: List[String]
                       , modelingProps: Option[String]) extends LazyLogging {
  /**
    * list of values for some parameters needed to initialize Karma
    */
  private val karmaDirParams: Map[ContextParameter,String] = Map(
    ContextParameter.PRELOADED_ONTOLOGY_DIRECTORY -> Paths.get(ModelerConfig.KarmaDir, "preloaded-ontologies/").toString,
    ContextParameter.USER_CONFIG_DIRECTORY -> Paths.get(ModelerConfig.KarmaDir, "config/").toString,
    // I have to explicitly add "/" at the end of the path because in Karma they create paths explicitly
    ContextParameter.ALIGNMENT_GRAPH_DIRECTORY -> (Paths.get(alignmentDir).toString + "/"),
    ContextParameter.GRAPHVIZ_MODELS_DIR -> (Paths.get(ModelerConfig.KarmaDir, "models-graphviz/").toString + "/"),
    ContextParameter.JSON_MODELS_DIR -> (Paths.get(ModelerConfig.KarmaDir, "models-json/").toString + "/"),

    ContextParameter.PYTHON_SCRIPTS_DIRECTORY -> Paths.get(ModelerConfig.KarmaDir, "python/").toString,
    ContextParameter.USER_UPLOADED_DIR -> Paths.get(ModelerConfig.KarmaDir, "user-uploaded-files/").toString,
    ContextParameter.USER_PREFERENCES_DIRECTORY -> (Paths.get(ModelerConfig.KarmaDir, "user-preferences/").toString + "/")
  )

  private val karmaInitParams: Map[ContextParameter,String] = Map(
    // taken from ../webapp/WEB-INF/web.xml
    ContextParameter.SRID_CLASS -> "SpatialReferenceSystem",
    ContextParameter.SRID_PROPERTY -> "hasSRID",
    ContextParameter.KML_CUSTOMIZATION_CLASS -> "KMLCustomization",
    ContextParameter.KML_CATEGORY_PROPERTY -> "hasKMLCategory",
    ContextParameter.KML_LABEL_PROPERTY -> "hasKMLLabel",
    ContextParameter.WGS84_LAT_PROPERTY -> "http://www.w3.org/2003/01/geo/wgs84_pos#lat",
    ContextParameter.WGS84_LNG_PROPERTY -> "http://www.w3.org/2003/01/geo/wgs84_pos#long",
    ContextParameter.POINT_POS_PROPERTY -> "http://www.opengis.net/gml/pos",
    ContextParameter.TRAINING_EXAMPLE_MAX_COUNT -> "100",
    ContextParameter.MSFT -> "False"
  ) ++ karmaDirParams

  /**
    * Create necessary folders for karma initialization
    */
  private def createDirs() = {
    karmaDirParams.values.foreach {
      str =>
        val f = Paths.get(str).toFile
        if (!f.exists) f.mkdirs
    }
  }

  /**
    * Write string to file.
    * If force, file will be always overwritten.
    * In other case it will only be written if it does not exist.
    *
    * @param karmaModelingProps String to be written
    * @param karmaConfigDir Path to the file
    * @param force Boolean
    * @return
    */
  private def writeModelingProps(karmaModelingProps: String,
                                 karmaConfigDir: Path,
                                 force: Boolean = false) = {
    Try {
      val modelingPath = Paths.get(karmaConfigDir.toString, "modeling.properties")
      if (force || !modelingPath.toFile.exists) {
        Files.write(modelingPath, karmaModelingProps.getBytes(StandardCharsets.UTF_8))
      }
    } match {
      case Success(_) =>
        logger.debug("Modeling props successfully written to file.")
      case Failure(err) =>
        logger.error(s"Modeling props could not be written to file: ${err.getMessage}")
        throw ModelerException("Modeling props could not be written to file")
    }

  }

  /**
    * copy karma modeling properties from the resources of our project.
    * should we do that or always use default modeling props?
    */
  private def copyModelingProps() = {

    val karmaConfigDir: Path = Paths.get(karmaInitParams
      .getOrElse(ContextParameter.USER_CONFIG_DIRECTORY,
        throw ModelerException("Karma config directory is not specified.")))

    modelingProps match {
      case Some(modelingString: String) =>
        logger.debug("Copying user-specified karma modeling properties")
        // in case modelingProps exist they will be overwritten
        writeModelingProps(modelingString, karmaConfigDir, true)
      case _ =>
        logger.debug("Copying default karma modeling properties")
        // in case modelingProps exist already nothing will be done
        writeModelingProps(ModelerConfig.defaultModelingProps, karmaConfigDir, false)
    }

  }

  private def setupOntoDir() = {
    // setup karma ontology directory
    val karmaOntoDir = Paths.get(karmaInitParams
      .getOrElse(ContextParameter.PRELOADED_ONTOLOGY_DIRECTORY,
        throw ModelerException("Ontology directory is not specified.")))
    // copy ontologies SSDStorage.ontologies to karma ontology directory
    ontologies.map {
      ontoPath =>
        logger.debug(s"Copying ontology $ontoPath to karma directory: ${karmaOntoDir.toString}")
        val dest = Paths.get(karmaOntoDir.toString
          , Paths.get(ontoPath).getFileName.toString)
        // NOTE: we replace the ontology if it already exists
        Files.copy(Paths.get(ontoPath), dest, StandardCopyOption.REPLACE_EXISTING)
    }
  }

  /**
    * initialize ServletContextParameterMap from karma-util
    */
  var karmaContextParameters: ServletContextParameterMap = {
    logger.debug(s"Setting up karma home directory at ${ModelerConfig.KarmaDir}")
    createDirs()
    setupOntoDir()
    // if we do not do the next step, then default modeling props will be used
    copyModelingProps()
//    val contextParams = ContextParametersRegistry.getInstance.getContextParameters(ModelerConfig.KarmaDir)
    val contextParams = ContextParametersRegistry.getInstance.getContextParameters(ModelerConfig.KarmaDir)

    // initialize context parameters according to default values
    logger.info("Initialize karma context parameters")
    karmaInitParams foreach {
      case (parameter,value) =>
        contextParams.setParameterValue(parameter, value)
    }
    contextParams
  }
  /**
    * initialize ModelingConfiguration in karma-common
    */
  var karmaModelingConfiguration: ModelingConfiguration = {
    logger.info("Initialize karma modeling configuration")
    ModelingConfigurationRegistry.getInstance
      .getModelingConfiguration(karmaContextParameters.getId)
  }

  /**
    * initialize karmaWorkspace
    * karmaWorkspace.getOntologyManager.getPrefixMap: I've changed Karma code by adding base prefix to the map
    */
  var karmaWorkspace: Workspace = {
    val workspace: Workspace = WorkspaceManager.getInstance.createWorkspace(karmaContextParameters.getId)
    WorkspaceRegistry.getInstance.register(new ExecutionController(workspace))
    WorkspaceKarmaHomeRegistry.getInstance.register(workspace.getId, karmaContextParameters.getKarmaHome)
    // to load the ontologies from the folder /preloaded-ontologies
    val omd: OntologyMetadata = new OntologyMetadata(karmaContextParameters)
    omd.setup(new UpdateContainer(), workspace)
    val outlierTag: Tag = new Tag(TagName.Outlier, Color.Red) // no idea what this is for!
    workspace.getTagsContainer.addTag(outlierTag)
    workspace
  }

  /**
    * Helper function to delete recursively a directory
    * @param path Path of the directory to be deleted
    */
  private def removeAll(path: Path): Unit = {
    def getRecursively(f: Path): Seq[Path] =
      f.toFile.listFiles
        .filter(_.isDirectory)
        .flatMap { x => getRecursively(x.toPath) } ++
        f.toFile.listFiles.map(_.toPath)
    getRecursively(path).foreach { f =>
      if (!f.toFile.delete) {throw ModelerException(s"Failed to delete ${f.toString}")}
    }
  }

  /**
    * delete karma home directory
    */
  def deleteKarma(): Unit = {
    logger.debug("Deleting karma home directory")
    removeAll(Paths.get(ModelerConfig.KarmaDir))

    // destroy all info cached at Karma side
    WorkspaceKarmaHomeRegistry.getInstance.deregister(karmaWorkspace.getId)
    WorkspaceRegistry.getInstance.deregister(karmaWorkspace.getId)
    WorkspaceManager.getInstance.removeWorkspace(karmaWorkspace.getId)
    ContextParametersRegistry.getInstance.deregister(karmaContextParameters.getId)
    ModelingConfigurationRegistry.getInstance.deregister(karmaContextParameters.getId)

  }

  /**
    * Read in a json file which stores semantic source description according to karma style
    * @param path String which indicates the location of the json file.
    * @return
    */
  def readKarmaModelJson(path: String) : KarmaSemanticModel = {
    KarmaSemanticModel(KarmaSsd.readJson(path))
  }

  /**
    * Create a map: prefix -> namespace
    * It includes prefixes which are specified in the preloaded-ontologies + karma + our default prefix.
    */
  def prefixMap: Map[String,String] = {
    // collection of prefixes and namespaces from the preloaded ontologies
    val m = karmaWorkspace.getOntologyManager
      .getPrefixMap.asScala // map: namespace -> prefix
    // we add the default namespace which is specified in the config file
    m += TypeConfig.DefaultNamespace -> "serene-default"
    m.map(_.swap) toMap
  }

  /**
    * Get a list of location strings of ontologies stored within Karma
    */
  def karmaOntologies: List[String] = {
    karmaInitParams
      .getOrElse(ContextParameter.PRELOADED_ONTOLOGY_DIRECTORY, None) match {
      case s: String =>
        val karmaOntoDir = Paths.get(s)
        if (!karmaOntoDir.toFile.exists) {
          logger.info("Karma preloaded ontology directory does not exist.")
          List()
        }
        else {
          karmaOntoDir.toFile.listFiles
            .filter(!_.isDirectory) // TODO: filter only ontology extensions!!!
            .map(_.getAbsolutePath) toList
        }
      case None =>
        logger.info("Karma preloaded ontology directory was not specified.")
        List()
    }
  }

}
