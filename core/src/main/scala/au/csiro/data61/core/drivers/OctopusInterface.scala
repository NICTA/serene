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
package au.csiro.data61.core.drivers

import java.nio.file.{Paths, Files, Path}

import au.csiro.data61.core.api._
import au.csiro.data61.core.drivers.Generic._
import au.csiro.data61.core.storage.OctopusStorage._
import au.csiro.data61.core.storage._
import au.csiro.data61.modeler.{PredictOctopus, TrainOctopus}
import au.csiro.data61.types.ColumnTypes.ColumnID
import au.csiro.data61.types.DataSetTypes._
import au.csiro.data61.types.ModelTypes.{Model, ModelID}
import au.csiro.data61.types.SsdTypes._
import au.csiro.data61.types.Training.{Status, TrainState}
import au.csiro.data61.types._
import com.typesafe.scalalogging.LazyLogging
import org.joda.time.DateTime

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ExecutionContext, Future}
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}
import scala.io.Source

/**
  * Interface to the functionality of the Semantic Modeler.
  * Here, requests will be sent to the ModelInterface as well.
  */
object OctopusInterface extends TrainableInterface[OctopusKey, Octopus] with LazyLogging {

  val DefaultNumSemanticTypes = 4

  override val storage = OctopusStorage

  protected def missingReferences(resource: Octopus): StorageDependencyMap = {
    // octopus depends on everything...

    // missing owls
    val owlIDs: List[OwlID] = resource.ontologies.filterNot(OwlStorage.keys.toSet.contains)

    // missing SSDs
    val ssdIDs: List[SsdID] = resource.ssds.filterNot(SsdStorage.keys.toSet.contains)

    // missing model
    val lobsterIDs: List[ModelID] = List(resource.lobsterID).filterNot(ModelStorage.keys.toSet.contains)

    StorageDependencyMap(owl = owlIDs, ssd = ssdIDs, model = lobsterIDs)

  }

  protected def dependents(resource: Octopus): StorageDependencyMap = {
    // octopus does not have dependents
    StorageDependencyMap()
  }

  /**
    * Check if the trained octopus is consistent.
    * This means that the alignment directory is available, lobster is consistent and that the SSDs
    * have not been updated since the octopus was last modified.
    *
    * @param id ID for the octopus
    * @return boolean
    */
  override def checkTraining(id: Key): Boolean = {
    logger.info(s"Checking consistency of octopus $id")

    val isOK = for {
      octopus <- get(id)
      trainDate = octopus.state.dateChanged
      refIDs = octopus.ssds
      refs = refIDs.flatMap(SsdStorage.get).map(_.dateModified)

      // associated schema matcher model is consistent
      lobsterConsistent = ModelInterface.checkTraining(octopus.lobsterID)
      // make sure the octopus is complete
      isComplete = octopus.state.status == Status.COMPLETE
      // make sure the SSDs are older than the training date
      allBefore = refs.forall(_.isBefore(trainDate))
      // make sure the alignment graph is there...
      alignmentExists = Files.exists(getAlignmentGraphPath(id))

    } yield allBefore && alignmentExists && isComplete && lobsterConsistent

    isOK getOrElse false
  }

  /**
    * Convert OctopusRequest to Octopus
    *
    * @param request
    * @param octopusID
    * @param lobsterID
    * @param semanticTypeMap
    * @return
    */
  protected def convertOctopusRequest(request: OctopusRequest,
                                      octopusID: OctopusID,
                                      lobsterID: ModelID,
                                      semanticTypeMap: Map[String, String]
                                     ): Octopus = {
    val now = DateTime.now
    Octopus(octopusID,
      name = request.name.getOrElse(MissingValue),
      ontologies = getOntologies(request.ssds, request.ontologies),
      ssds = request.ssds.getOrElse(List.empty[Int]),
      lobsterID = lobsterID,
      modelingProps = request.modelingProps.getOrElse(ModelingProperties()), // default will be created
      semanticTypeMap = semanticTypeMap,
      state = TrainState(Status.UNTRAINED, "", now),
      dateCreated = now,
      dateModified = now,
      description = request.description.getOrElse(MissingValue)
    )
  }

  /**
    * Build a new Octopus from the request.
    * We also build the associated schema matcher Model.
    *
    * @param request The request object from the API
    * @return
    */
  def createOctopus(request: OctopusRequest): Try[Octopus] = Try {

    val id = genID


    val brokenRules = request.modelingProps.map(_.brokenRules).getOrElse(Nil)
    if (brokenRules.nonEmpty) {
      throw BadRequestException(brokenRules.mkString(" "))
    }

    val modelingProps = request.modelingProps.getOrElse(ModelingProperties())

    val SemanticTypeObject(classes, labelData, semanticTypeMap) = getSemanticTypes(request.ssds)

    // we need first to create the schema matcher model
    val modelReq = ModelRequest(
      description = request.description,
      modelType = request.modelType,
      classes = classes,
      features = request.features,
      costMatrix = None,
      labelData = labelData, // add unknown class columns?
      resamplingStrategy = request.resamplingStrategy,
      numBags = request.numBags,
      bagSize = request.bagSize)

    val lobsterID: ModelID = Try {
      ModelInterface.createModel(modelReq)
    } match {
      case Success(model) =>
        model.id
      case Failure(err) =>
        logger.error(s"Octopus cannot be created since lobster fled: ${err.getMessage}")
        throw InternalException(s"Octopus cannot be created since lobster fled.")
    }

    // build the octopus from the request, adding defaults where necessary
    val octopus = convertOctopusRequest(request, id, lobsterID, semanticTypeMap)

    add(octopus) match {
      case Some(key) =>
        octopus
      case None =>
        logger.error(s"Failed to create resource.")
        throw InternalException(s"Failed to create resource.")
    }

  }

  /**
    * Get octopus and the associated schema matcher model.
    * In case any of those two are missing throw NotFound.
    *
    * @param id
    * @return
    */
  protected def getModels(id: OctopusID): (Octopus, Model) = {
    val octopus: Octopus = OctopusStorage.get(id) match {
      case Some(o: Octopus) => o
      case _ =>
        logger.error(s"Octopus $id not found.")
        throw NotFoundException(s"Octopus $id not found.")
    }
    val model: Model = ModelStorage.get(octopus.lobsterID) match {
      case Some(m: Model) => m
      case _ =>
        logger.error(s"Associated model ${octopus.lobsterID} for octopus $id not found.")
        throw NotFoundException(s"Associated model ${octopus.lobsterID} for octopus $id not found.")
    }

    (octopus, model)
  }

  /**
    * Trains the Octopus which includes training for the Schema Matcher and Semantic Modeler!
    *
    * @param id The octopus id
    * @return
    */
  def trainOctopus(id: OctopusID, force: Boolean = false): Option[TrainState] = {

    val (octopus, model) = getModels(id)

    if (checkTraining(id) && !force) {
      logger.info(s"Octopus $id is already trained.")
      Some(octopus.state)
    } else if (octopus.state.status == Status.BUSY) {
      logger.info(s"Octopus $id is busy.")
      Some(octopus.state)
    } else {
      logger.info("Launching training of the octopus and lobster")
      // first we set the model state to training....
      OctopusStorage.updateTrainState(id, Status.BUSY)
      // launch training for the lobster
      val launchLobster = ModelInterface.lobsterTraining(model.id, force)
      // launch training for the octopus
      val launchOctopus = launchOctopusTraining(id)

      // merge both futures
      val aggFut: Future[(Option[Path], Option[Path])] = for {
        futureLobsterPath <- launchLobster
        futureOctopusPath <- launchOctopus
      } yield (futureLobsterPath, futureOctopusPath)

      aggFut onComplete {
        case Success(res: (Option[Path], Option[Path])) =>
          processPaths(id, model.id, res)
        case Failure(err) =>
          err.printStackTrace()
          val msg = s"Failed to train octopus $id: ${err.getMessage}."
          logger.error(msg)
          ModelStorage.updateTrainState(model.id, Status.ERROR, msg, None)
          OctopusStorage.deleteAlignmetDir(id) // delete alignmentDir
          OctopusStorage.updateTrainState(id, Status.ERROR, msg, None)
      }

      OctopusStorage.get(id).map(_.state)
    }
  }

  /**
    * Processing paths as returned by training methods
    *
    * @param octopusID Octopus id
    * @param lobsterID Id of the schema matcher model
    * @param res Tuple of paths wrapped into Option
    * @return
    */
  private def processPaths(octopusID: OctopusID,
                           lobsterID: ModelID,
                           res: (Option[Path], Option[Path])): Option[TrainState] = {
    res match {
      case (Some(lobsterPath), Some(octopusPath)) =>
        logger.info(s"Octopus $octopusID training success")
        // we update the status, the state date and do not delete the model.rf file for the lobster
        ModelStorage.updateTrainState(lobsterID, Status.COMPLETE, "", Some(lobsterPath))
        // we update the status, the state date for the octopus
        OctopusStorage.updateTrainState(octopusID, Status.COMPLETE, "", Some(octopusPath))

      case (Some(lobsterPath), None) =>
        logger.error(s"Octopus $octopusID training unsuccessful: matcher succeeded, but modeler failed.")
        // we update the status, the state date and do not delete the model.rf file for the lobster
        ModelStorage.updateTrainState(lobsterID, Status.COMPLETE, "", Some(lobsterPath))
        // we set the octopus state to error
        OctopusStorage.deleteAlignmetDir(octopusID) // delete alignmentDir
        OctopusStorage.updateTrainState(octopusID, Status.ERROR, "Modeler failed.", None)

      case (None, Some(octopusPath)) =>
        logger.error(s"Octopus $octopusID training unsuccessful: modeler succeeded, but matcher failed.")
        // we set lobster state to error
        ModelStorage.updateTrainState(lobsterID, Status.ERROR, "MatcherError", None)
        // we set the octopus state to error
        OctopusStorage.updateTrainState(octopusID, Status.ERROR, "Matcher failed.", None)

      case (None, None) =>
        logger.error(s"Octopus $octopusID training unsuccessful: modeler and matcher failed.")
        // we set lobster state to error
        ModelStorage.updateTrainState(lobsterID, Status.ERROR, "MatcherError", None)
        // we set the octopus state to error
        OctopusStorage.deleteAlignmetDir(octopusID) // delete alignmentDir
        OctopusStorage.updateTrainState(octopusID, Status.ERROR, "Matcher and Modeler failed.", None)
    }
  }

  /**
    * Asynchronously launch the training process,
    * and alignment graph will be written to the storage layer during execution.
    * The actual state will be returned from the above case when re-read from the storage layer.
    *
    * @param id Octopus id for which training will be launched
    */
  private def launchOctopusTraining(id: OctopusID)(implicit ec: ExecutionContext): Future[Option[Path]] = {

    Future {

      val octopus = get(id).get

      // get SSDs for the training
      val knownSSDs: List[Ssd] = octopus.ssds.flatMap(SsdStorage.get)

      // get location strings of the ontologies
      val ontologies = octopus.ontologies
        .flatMap(OwlStorage.getOwlDocumentPath)
        .map(_.toString)

      // proceed with training...
      val finalPath = TrainOctopus.train(
        octopus,
        OctopusStorage.getAlignmentDirPath(id),
        ontologies,
        knownSSDs
      )
      logger.info(s"Train complete: $finalPath")
      finalPath
    }
  }

  /**
    * Perform prediction using a given octopus for a given SSD.
    *
    * @param id The octopus id
    * @param ssdID id of the SSD
    * @return
    */
  def predictSsdOctopus(id: OctopusID, ssdID : SsdID): SsdPrediction = {

    if (checkTraining(id)) {
      // do prediction
      logger.info(s"Launching prediction for OCTOPUS $id...")
      val octopus: Octopus = OctopusStorage.get(id).get

      // we get here attributes which are transformed columns
      val ssdColumns: List[ColumnID] = SsdStorage.get(ssdID).get.attributes.map(_.id)
      val datasets: List[DataSetID] =
        DatasetStorage.columnMap
          .filterKeys(ssdColumns.contains)
          .values
          .map(_.datasetID)
          .toList
          .distinct

      if (datasets.size > 1) {
        logger.error("Octopus prediction for more than one dataset is not supported yet.")
        throw InternalException("Octopus prediction for more than one dataset is not supported yet.")
      }

      // we do semantic typing for only one dataset
      val dsPredictions = Try {
        ModelInterface.predictModel(octopus.lobsterID, datasets.head)
      } toOption

      // this map is needed to map ColumnIDs from dsPredictions to attributes
      // we make the mappings identical
      val attrToColMap: Map[Int, Int] = SsdStorage.get(ssdID)
        .map(_.attributes.map(x => (x.id, x.id)).toMap)
        .getOrElse(Map.empty[Int, Int])

      PredictOctopus.predict(octopus,
        OctopusStorage.getAlignmentDirPath(octopus.id).toString,
        octopus.ontologies
          .flatMap(OwlStorage.getOwlDocumentPath)
          .map(_.toString),
        SsdStorage.get(ssdID).get,
        dsPredictions,
        attrToColMap,
        getNumSemanticTypes(octopus.modelingProps))
        .getOrElse(throw InternalException(s"No SSD predictions are available for octopus $id."))

    } else {
      val msg = s"Prediction failed. Octopus $id is not trained."
      // prediction is impossible since the model has not been trained properly
      logger.error(msg)
      throw BadRequestException(msg)
    }
  }

  /**
    * Get Number of semantic types which will be used to construct mappings.
    *
    * @param modelingProperties
    * @return
    */
  private def getNumSemanticTypes(modelingProperties: ModelingProperties): Int = {
//    modelingProperties.map(_.numSemanticTypes).getOrElse(DefaultNumSemanticTypes)
    modelingProperties.numSemanticTypes
  }

  /**
    * Generate an empty SSD for a given dataset using ontologies from a given octopus.
    *
    * @param octopus Octopus object
    * @param dataset Dataset object
    * @return
    */
  private def generateEmptySsd(octopus: Octopus,
                       dataset: DataSet): Option[Ssd] = {
    logger.debug(s"Generating empty SSD for dataset ${dataset.id}")
    Try {
      Ssd(id = genID,
        name = dataset.filename,
        attributes = dataset
          .columns
          .map(col => SsdAttribute(id = col.id, name = col.name)),
        ontologies = octopus.ontologies,
        semanticModel = None,
        mappings = None,
        dateCreated = DateTime.now,
        dateModified = DateTime.now
      )
    } toOption
  }

  /**
    * Helper function to convert from Ssd type to SsdRequest.
    * @param ssd Semantic Source Description
    * @return
    */
  private def convertSsd(ssd: Ssd): SsdRequest = {
    SsdRequest(Some(ssd.name), Some(ssd.ontologies), ssd.semanticModel, ssd.mappings)
  }

  /**
    * Helper function convert from SsdPrediction to SsdResults
    *
    * @param ssdPrediction Prediction object as returned by semantic modeler
    * @return
    */
  private def convertSsdPrediction(ssdPrediction: SsdPrediction): SsdResults = {
    val convertedTuples =
      ssdPrediction.suggestions.map {
        case (ssd: Ssd, smScore: SemanticScores) =>
          SsdResult(convertSsd(ssd), smScore)
      }
    SsdResults(predictions = convertedTuples)
  }

  /**
    * Get octopus and dataset which are needed for prediction.
    * If octopus or dataset do not exist in the storage, exception will be thrown.
    * @param id
    * @param dsId
    * @return
    */
  private def getPredictionResources(id: OctopusID,
                                     dsId : DataSetID
                                    ): (Octopus, DataSet) = {
    val octopus = OctopusStorage.get(id) match {
      case Some(octo) =>
        octo
      case None =>
        throw NotFoundException(s"Octopus $id not found.")
    }
    val dataset = DatasetStorage.get(dsId) match {
      case Some(ds) =>
        ds
      case None =>
        throw NotFoundException(s"Dataset $dsId not found.")
    }

    (octopus, dataset)
  }

  /**
    * Perform prediction using the octopus for a dataset.
    * Emtpy SSD will automatically be generated for the dataset.
    * If the column has a high probability to be unknown,
    * it is discarded from the semantic modelling step.
    * @param id The octopus id
    * @param dsId id of the dataset
    * @return
    */
  def predictOctopus(id: OctopusID, dsId : DataSetID): SsdResults = {
    val (octopus, dataset) = getPredictionResources(id, dsId)

    if (checkTraining(id)) {
      // do prediction
      logger.info(s"Launching prediction with octopus $id for dataset $dsId")

      val ssdPredictions: Option[SsdPrediction] = for {

        emptySsd <- generateEmptySsd(octopus, dataset)

        // we do semantic typing for only one dataset
        // TODO: throw error if prediction with schema matcher fails
//        dsPredictions = Try {
//          ModelInterface.predictModel(octopus.lobsterID, dataset.id)
//        } toOption

        dsPredictions = ModelInterface.predictModel(octopus.lobsterID, dataset.id)


        // this map is needed to map ColumnIDs from dsPredictions to attributes
        // we make the mappings identical
        attrToColMap: Map[Int, Int] = emptySsd
          .attributes
          .map(x => (x.id, x.id)).toMap

        predOpt <- PredictOctopus.predict(octopus,
          OctopusStorage.getAlignmentDirPath(octopus.id).toString,
          octopus.ontologies
            .flatMap(OwlStorage.getOwlDocumentPath)
            .map(_.toString),
          emptySsd,
          Some(dsPredictions),
          attrToColMap,
          getNumSemanticTypes(octopus.modelingProps)
        )
      } yield predOpt

      convertSsdPrediction(
        ssdPredictions
        .getOrElse(throw InternalException(s"No SSD predictions are available for dataset $dsId."))
      )

    } else {
      val msg = s"Prediction failed. Octopus $id is not trained."
      // prediction is impossible since the model has not been trained properly
      logger.error(msg)
      throw BadRequestException(msg)
    }
  }

  /**
    * Split URI into the namespace and the name of the resource (either class, or property)
    *
    * @param s String which corresponds to the URI
    * @return
    */
  protected def splitURI(s: String): (String, String) = {
    Try { SsdTypes.splitURI(s) } match {
      case Success((label, namespace)) =>
        (label, namespace)
      case Failure(err) =>
        logger.error(s"Failed to process the URI $s")
        throw InternalException(s"Failed to process the URI $s")
    }
  }

  /**
    * SemanticTypeObject is the return object for getSemanticTypes
    *
    * @param semanticTypeList List of semantic types (aka classes)
    * @param labelMap Mapping from columns to semantic types (aka labelData = training data for schema matcher)
    * @param semanticTypeMap Mapping from semantic types to URI namespaces
    */
  case class SemanticTypeObject(semanticTypeList: Option[List[String]],
                                labelMap: Option[Map[Int, String]],
                                semanticTypeMap: Map[String, String])

  /**
    * We want to extract the list of semantic types (aka classes),
    * the list of mappings from column ids to the semantic types (aka labelData) and
    * the list of mappings from the semantic types to the URI namespaces.
    * The semantic types in the SSDs are provided as URIs.
    * That's why we need to split those URIs into namespace and semantic type names.
    * The semantic type names will be used as class labels in the Schema Matcher.
    * The namespaces are needed for the Semantic Modeler to properly work with ontologies.
    * @param ssds List of semantic source descriptions.
    * @return
    */
  protected def getSemanticTypes(ssds: Option[List[Int]]): SemanticTypeObject = {

    logger.debug("Getting semantic type info from the octopus...")
    val givenSSDs = ssds.getOrElse(List.empty[Int])

    if (givenSSDs.isEmpty) {
      // everything is empty if there are no SSDs
      logger.warn("Everything is empty in SSD.")
      SemanticTypeObject(None, None, Map.empty[String,String])
    }
    else {
      // here we want to get a map from AttrID to (URI of class node, URI of data node)
      val ssdMaps: List[(AttrID, (String, String))] = processSsdMappings(givenSSDs)

      logger.debug(s"semantic labels for attributes: $ssdMaps")

      val semanticTypeMap: Map[String, String] = ssdMaps.flatMap {
        // TODO: what if we have the same labels with different namespaces? Only one will be picked here
        case (attrID, (classURI, propURI)) =>
          List(splitURI(classURI), splitURI(propURI))
      }.toMap

      logger.debug(s"semantic type map: $semanticTypeMap")

      // semantic type (aka class label) is constructed as "the label of class URI"---"the label of property URI"
      // TODO: add unknown class columns
      val labelData: Map[Int, String] = ssdMaps.map {
        case (attrID: Int, (classURI, propURI)) =>
          (attrID, constructLabel(classURI,propURI))
      } toMap

      logger.debug(s"label data: $labelData")

      val classes: List[String] = (labelData.map {
        case (attrID, semType) =>
          semType
      }.toList :+ ModelTypes.UknownClass).distinct

      SemanticTypeObject(Some(classes), Some(labelData), semanticTypeMap)
    }
  }

  /**
    * Helper function to convert mappings {attrID -> nodeID} to
    * the format {attrID -> (ClassURI, PropURI)}
    * @param givenSSDs List of ids for ssds
    * @return
    */
  protected def processSsdMappings(givenSSDs: List[SsdID]):List[(AttrID, (String, String))] = {
    givenSSDs
      .flatMap(SsdStorage.get)
      .filter(_.mappings.isDefined)
      .filter(_.semanticModel.isDefined)
      .flatMap { ssd: Ssd =>
        logger.debug(s"    working on ssd: (${ssd.id}, ${ssd.name})")
        ssd.mappings.get.mappings.map { // SSDs contain SSDMapping which is a mapping AttrID --> NodeID
          case (attrID, nodeID) =>
            (attrID,
              ssd.semanticModel
                .flatMap(_.getDomainType(nodeID))
                .getOrElse(throw InternalException(
                "Semantic Source Description is not properly formed: problems with mappings or semantic model.")))
        } toList
      }
  }

  /**
    * Construct the semantic type (aka class label) as
    * "the label of class URI"---"the label of property URI"
    *
    * @param classURI URI of the class node
    * @param propURI URI of the data node
    * @return
    */
  protected def constructLabel(classURI: String, propURI: String): String = {
    splitURI(classURI)._1 + "---" + splitURI(propURI)._1
  }

  /**
    * Get the list of OWL ids based on the list of SSDs and list of additional OWL ids.
    * The point is that the user can specify additional ontologies to those which are already specified in the SSDs.
    * We still need to get ontologies from the provided SSDs.
    *
    * @param ssds List of ids of the semantic source descriptions.
    * @param ontologies List of ids of the ontologies which need to be additionally included into the Octopus.
    * @return
    */
  protected def getOntologies(ssds: Option[List[Int]],
                              ontologies: Option[List[Int]]): List[Int] = {
    logger.debug("Getting owls for octopus")
    val ssdOntologies: List[Int] = ssds
      .getOrElse(List.empty[Int])
      .flatMap(SsdStorage.get)
      .flatMap(_.ontologies)

    (ssdOntologies ++ ontologies.getOrElse(List.empty[Int])).distinct
  }


  /**
    * Deletes the octopus
    *
    * @param key Key for the octopus
    * @return
    */
  def deleteOctopus(key: OctopusID): Option[OctopusID] = {
    // we store the ID of the associated model
    val lobsterID: Option[ModelID] = OctopusStorage.get(key).map(_.lobsterID)
    // first we remove octopus
    val octopusID = delete(key)
    // now we remove lobster
    lobsterID.map(ModelInterface.delete(_))

    octopusID
  }

  /**
    * Get alignment graph for the octopus
    *
    * @param key Key for the octopus
    * @return
    */
  def getAlignment(key: OctopusID): Option[String] = {
    // we store the ID of the associated model
    val alignmentPath: Path = OctopusStorage.getAlignmentGraphPath(key)

    if (!alignmentPath.toFile.exists){
      None
    }

    val fileContents = Source.fromFile(alignmentPath.toString).getLines.mkString
    Some(fileContents)
  }

  protected def createModelRequest(request: OctopusRequest,
                                   lobster: Model,
                                   classes: Option[List[String]],
                                   labelData: Option[Map[Int, String]]): ModelRequest = {
    ModelRequest(
        description = Some(request.description.getOrElse(lobster.description)),
        modelType = Some(request.modelType.getOrElse(lobster.modelType)),
        classes = Some(classes.getOrElse(lobster.classes)),
        features = Some(request.features.getOrElse(lobster.features)),
        costMatrix = None,
        labelData = Some(labelData.getOrElse(lobster.labelData)),
        resamplingStrategy = Some(request.resamplingStrategy.getOrElse(lobster.resamplingStrategy)),
        numBags = Some(request.numBags.getOrElse(lobster.numBags)),
        bagSize = Some(request.bagSize.getOrElse(lobster.bagSize))
    )
  }

  /**
    * Parses a octopus request to construct a octopus object
    * for updating. The index is searched for in the database,
    * and if update is successful, returns the case class response
    * object.
    * The associated schema matcher model (aka lobster) is also updated.
    * Since octopus and lobster are bundled now, the order of update matters.
    *
    * @param request POST request with octopus information
    * @return Case class object for JSON conversion
    */
  def updateOctopus(id: OctopusID, request: OctopusRequest): Octopus = {

    val brokenRules = request.modelingProps.map(_.brokenRules).getOrElse(Nil)
    if (brokenRules.nonEmpty) {
      logger.warn(s"Rules are broken for octopus update: ${brokenRules.mkString(" ")}")
      throw BadRequestException(brokenRules.mkString(" "))
    }

    val (original, originalLobster) = getModels(id)

    val ssds = request.ssds.getOrElse(original.ssds)
    val SemanticTypeObject(classes, labelData, semanticTypeMap) = getSemanticTypes(Some(ssds))

    // build the octopus from the request, adding defaults where necessary
    val octopusOpt = for {
      // delete alignmentDir explicitly
      octopusDeletedAlignment <- OctopusStorage.deleteAlignmetDir(id)

      // build up the octopus request, and use defaults if not present...
      octopus <- Try {
        Octopus(
          id = id,
          lobsterID = original.lobsterID,
          description = request.description.getOrElse(original.description),
          name = request.name.getOrElse(original.name),
          modelingProps = request.modelingProps.getOrElse(original.modelingProps),
          ssds = ssds,
          ontologies = request.ontologies.getOrElse(original.ontologies),
          semanticTypeMap = semanticTypeMap,
          state = TrainState(Status.UNTRAINED, "", DateTime.now),
          dateCreated = original.dateCreated,
          dateModified = DateTime.now)
      }.toOption
      _ <- update(octopus)

    } yield octopus

    // update lobster after the update of the octopus
    // in case octopus update fails we should not update lobster
    val lobsterRequest = createModelRequest(request, originalLobster, classes, labelData)
    val updatedLobster = ModelInterface.updateModel(original.lobsterID, lobsterRequest)

    octopusOpt getOrElse {
      logger.error(s"Failed to update octopus $id.")
      throw InternalException("Failed to update octopus.")
    }
  }

}
