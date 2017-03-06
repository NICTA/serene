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

import java.io.{IOException, InputStream}
import java.nio.file.{Path, Paths}

import au.csiro.data61.core.api._
import au.csiro.data61.core.storage._
import au.csiro.data61.modeler.{PredictOctopus, TrainOctopus}
import au.csiro.data61.types.ModelTypes.{Model, ModelID}
import au.csiro.data61.core.drivers.Generic._
import au.csiro.data61.types.ColumnTypes.ColumnID
import au.csiro.data61.types.SsdTypes.OwlDocumentFormat.OwlDocumentFormat
import au.csiro.data61.types._
import au.csiro.data61.types.DataSetTypes._
import au.csiro.data61.types.Training.{Status, TrainState}
import au.csiro.data61.types.SsdTypes._
import com.twitter.io.Reader
import com.typesafe.scalalogging.LazyLogging
import org.joda.time.DateTime

import scala.language.postfixOps
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Random, Success, Try}

/**
  * Interface to the functionality of the Semantic Modeler.
  * Here, requests will be sent to the MatcherInterface as well.
  */
object OctopusInterface extends StorageInterface[OctopusID, Octopus] with Trainable with LazyLogging {

  val MissingValue = "unknown"
  val defaultNumSemanticTypes = 4 // TODO: make it a user-provided parameter --> move to modelingProps

  override val storage = OctopusStorage

  protected def missingReferences[Octopus](resource: Octopus): StorageDependencyMap = Map()

  protected def dependents[Octopus](resource: Octopus): StorageDependencyMap = Map()

  override def checkTraining: Boolean = {
    true
  }

  /**
    * Check if the SsdRequest has proper parameters.
    *
    * @param request SsdRequest coming from the API
    * @param ssdAttributes generated attributes
    * @throws InternalException if there;s a problem
    */
  protected def checkSsdRequest(request: SsdRequest,
                                ssdAttributes: List[SsdAttribute])
  : Unit = {

    // check that columnIDs are available in the DataSetStorage
    if (!ssdAttributes.forall(attr => DatasetStorage.columnMap.keySet.contains(attr.id))) {
      val msg = "SSD cannot be added to the storage: columns in the mappings do not exist in the DatasetStorage."
      logger.error(msg)
      throw BadRequestException(msg)
    }

    // check that ontologies are available in the OwlStorage
    if (!request.ontologies.forall(OwlInterface.owlKeys.toSet.contains)) {
      val msg = "SSD cannot be added to the storage: ontologies do not exist in the OwlStorage."
      logger.error(msg)
      throw BadRequestException(msg)
    }
  }

  /**
    * Check if the SsdRequest has proper parameters.
    * Then convert it to Semantic Source Description
    *
    * @param request SsdRequest coming from the API
    * @param ssdAttributes List of generated attributes
    * @param ssdID id of the SSD
    */
  protected def convertSsdRequest(request: SsdRequest,
                                  ssdAttributes: List[SsdAttribute],
                                  ssdID: SsdID)
  : Ssd = {

    val ssd = Ssd(ssdID,
      name = request.name,
      attributes = ssdAttributes,
      ontology = request.ontologies,
      semanticModel = request.semanticModel,
      mappings = request.mappings,
      dateCreated = DateTime.now,
      dateModified = DateTime.now
    )

    // check if the SSD is consistent and complete
    if (!ssd.isComplete) {
      val msg = "SSD cannot be added to the storage: it is not complete."
      logger.error(msg)
      throw BadRequestException(msg)
    }

  ssd
  }

  /**
    * Check the request and if it's ok add a corresponding SSD to the storage.
    *
    * @param request Request object
    * @return
    */
  def createSsd(request: SsdRequest): Ssd = {

    val id = genID

    // get list of attributes from the mappings
    // NOTE: for now we automatically generate them to be equal to the original columns
    val ssdAttributes: List[SsdAttribute] = request
      .mappings
      .map(_.mappings.keys.toList)
      .getOrElse(List.empty[Int])
      .map(SsdAttribute(_))

    // check the SSD request -- the method will just raise exceptions if there's anything wrong
    checkSsdRequest(request, ssdAttributes)

    // build the Semantic Source Description from the request, adding defaults where necessary
    val ssdOpt = for {
      ssd <- Try {
        // we convert here the request to the SSD and also check if the SSD is complete
        convertSsdRequest(request, ssdAttributes, id)
      }.toOption
      _ <- SsdStorage.add(id, ssd)

    } yield ssd

    ssdOpt getOrElse { throw InternalException("Failed to create resource.") }

  }

  /**
    * Build a new Octopus from the request.
    * We also build the associated schema matcher Model.
    *
    * @param request The request object from the API
    * @return
    */
  def createOctopus(request: OctopusRequest): Octopus = {

    val id = genID

    val SemanticTypeObject(classes, labelData, semanticTypeMap) = getSemanticTypes(request.ssds)

    // we need first to create the schema matcher model
    val modelReq = ModelRequest(
      description = request.description,
      modelType = request.modelType,
      classes = classes,
      features = request.features,
      costMatrix = None,
      labelData = labelData,
      resamplingStrategy = request.resamplingStrategy,
      numBags = request.numBags,
      bagSize = request.bagSize)

    val lobsterID: ModelID = Try {
      MatcherInterface.createModel(modelReq)
    } match {
      case Success(model) =>
        model.id
      case Failure(err) =>
        throw InternalException(s"Octopus cannot be created since lobster fled: $err")
    }

    // build the octopus from the request, adding defaults where necessary
    val octopusOpt = for {

      octopus <- Try {
        Octopus(id,
          name = request.name.getOrElse(MissingValue),
          ontologies = getOntologies(request.ssds, request.ontologies),
          ssds = request.ssds.getOrElse(List.empty[Int]),
          lobsterID = lobsterID,
          modelingProps = request.modelingProps,
          semanticTypeMap = semanticTypeMap,
          state = TrainState(Status.UNTRAINED, "", DateTime.now),
          dateCreated = DateTime.now,
          dateModified = DateTime.now,
          description = request.description.getOrElse(MissingValue)
        )
      }.toOption
      _ <- OctopusStorage.add(id, octopus)

    } yield octopus

    octopusOpt getOrElse { throw InternalException("Failed to create resource.") }
  }

  /**
    * Trains the Octopus which includes training for the Schema Matcher and Semantic Modeler!
    *
    * @param id The octopus id
    * @return
    */
  def trainOctopus(id: OctopusID, force: Boolean = false): Option[TrainState] = {

    val octopus = OctopusStorage.get(id).get
    val model = ModelStorage.get(octopus.lobsterID).get

    if (OctopusStorage.isConsistent(id) && !force) {
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
      val launchLobster = MatcherInterface.lobsterTraining(model.id, force)
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
      val octopus = OctopusStorage.get(id).get

      // get SSDs for the training
      val knownSSDs: List[Ssd] = octopus.ssds.flatMap(SsdStorage.get)

      // get location strings of the ontologies
      val ontologies: List[String] = octopus.ontologies
        .flatMap(OwlStorage.getOwlDocumentPath)
        .map(_.toString)

      // proceed with training...
      TrainOctopus.train(octopus,
        OctopusStorage.getAlignmentDirPath(id),
        ontologies,
        knownSSDs
      )
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

    if (OctopusStorage.isConsistent(id)) {
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
        MatcherInterface.predictModel(octopus.lobsterID, datasets.head)
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
        defaultNumSemanticTypes)
        .getOrElse(throw InternalException(s"No SSD predictions are available for octopus $id."))

    } else {
      val msg = s"Prediction failed. Octopus $id is not trained."
      // prediction is impossible since the model has not been trained properly
      logger.error(msg)
      throw BadRequestException(msg)
    }
  }

  /**
    * Generate an empty SSD for a given dataset using ontologies from a given octopus.
    *
    * @param octopus Octopus object
    * @param dataset Dataset object
    * @return
    */
  protected def generateEmptySsd(octopus: Octopus,
                       dataset: DataSet): Option[Ssd] = {
    Try {
      Ssd(id = genID,
        name = dataset.filename,
        attributes = dataset.columns.map(_.id).map(SsdAttribute(_)),
        ontology = octopus.ontologies,
        semanticModel = None,
        mappings = None,
        dateCreated = DateTime.now,
        dateModified = DateTime.now
      )
    } toOption
  }

  /**
    * Helper function to convert from Ssd type to SsdRequest.
    *
    * @param ssd Semantic Source Description
    * @return
    */
  protected def convertSsd(ssd: Ssd): SsdRequest = {
    SsdRequest(ssd.name,
      ssd.ontology,
      ssd.semanticModel,
      ssd.mappings)
  }

  /**
    * Helper function convert from SsdPrediction to SsdResults
    *
    * @param ssdPrediction Prediction object as returned by semantic modeler
    * @return
    */
  protected def convertSsdPrediction(ssdPrediction: SsdPrediction): SsdResults = {
    val convertedTuples =
      ssdPrediction.suggestions.map {
        case (ssd: Ssd, smScore: SemanticScores) =>
          (convertSsd(ssd), smScore)
      }
    SsdResults(predictions = convertedTuples)
  }

  /**
    * Perform prediction using the octopus for a dataset.
    * Emtpy SSD will automatically be generated for the dataset.
    *
    * @param id The octopus id
    * @param dsId id of the dataset
    * @return
    */
  def predictOctopus(id: OctopusID, dsId : DataSetID): SsdResults = {

    if (OctopusStorage.isConsistent(id)) {
      // do prediction
      logger.info(s"Launching prediction with octopus $id for dataset $dsId")
      val ssdPredictions: Option[SsdPrediction] = for {
        octopus <- OctopusStorage.get(id)
        dataset <- DatasetStorage.get(dsId)

        emptySsd <- generateEmptySsd(octopus, dataset)

        // we do semantic typing for only one dataset
        dsPredictions = Try {
          MatcherInterface.predictModel(octopus.lobsterID, dsId)
        } toOption

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
          dsPredictions,
          attrToColMap,
          defaultNumSemanticTypes
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
    val splitted = s.split("#")

    if (splitted.length < 2){
      logger.error(s"Failed to process the URI $s")
      throw InternalException(s"Failed to process the URI $s")
    }

    // the first part is the name which will be later used in the schema matcher as the label
    // the second part is the namespace
    ( splitted.last, splitted.dropRight(1).mkString("#") + "#")
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
    *
    * @param ssds List of semantic source descriptions.
    * @return
    */
  protected def getSemanticTypes(ssds: Option[List[Int]]): SemanticTypeObject = {

    logger.debug("Getting semantic type info from the octopus...")
    val givenSSDs = ssds.getOrElse(List.empty[Int])

    if (givenSSDs.isEmpty) {
      // everything is empty if there are no SSDs
      logger.debug("Everything is empty in SSD.")
      SemanticTypeObject(None, None, Map.empty[String,String])
    }
    else {
      // here we want to get a map from AttrID to (URI of class node, URI of data node)
      // TODO: the mapping can be to the ClassNode
      val ssdMaps: List[(AttrID, (String, String))] = givenSSDs
        .flatMap(SsdStorage.get)
        .filter(_.mappings.isDefined)
        .filter(_.semanticModel.isDefined)
        .flatMap { ssd: Ssd =>
          ssd.mappings.get.mappings.map { // SSDs contain SSDMapping which is a mapping AttrID --> NodeID
            case (attrID, nodeID) =>
              (attrID,
                ssd.semanticModel
                  .flatMap(_.getDomainType(nodeID)) // FIXME: what if the mapping is to the class node and not the data node?!
                  .getOrElse(throw InternalException(
                    "Semantic Source Description is not properly formed: problems with mappings or semantic model.")))
            } toList
        }

      val semanticTypeMap: Map[String, String] = ssdMaps.flatMap {
        // TODO: what if we have the same labels with different namespaces? Only one will be picked here
        case (attrID, (classURI, propURI)) =>
          List(splitURI(classURI), splitURI(propURI))
      }.toMap

      // semantic type (aka class label) is constructed as "the label of class URI"---"the label of property URI"
      // TODO: what if the column is mapped to the class node?
      val labelData: Map[Int, String] = ssdMaps.map {
        case (attrID: Int, (classURI, propURI)) =>
          (attrID, constructLabel(classURI,propURI))
      } toMap

      val classes: List[String] = labelData.map {
        case (attrID, semType) =>
          semType
      }.toList.distinct

      SemanticTypeObject(Some(classes), Some(labelData), semanticTypeMap)
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
      .flatMap(_.ontology)

    ssdOntologies ++ ontologies.getOrElse(List.empty[Int])
  }

  /**
    * Passes the ssd keys up to the API
    *
    * @return
    */
  def ssdKeys: List[SsdID] = {
    SsdStorage.keys
  }

  /**
    * Passes the octopus keys up to the API
    *
    * @return
    */
  def octopusKeys: List[OctopusID] = {
    OctopusStorage.keys
  }

  /**
    * Deletes the octopus
    *
    * @param key Key for the octopus
    * @return
    */
  def deleteOctopus(key: OctopusID): Option[OctopusID] = {
    if(OctopusStorage.hasDependents(key)) {
      val msg = s"Octopus $key cannot be deleted since it has dependents." +
        s"Delete first dependents."
      logger.error(msg)
      throw BadRequestException(msg)
    } else {
      // first we remove octopus
      val octopusID = OctopusStorage.remove(key)
      // now we remove lobster
      OctopusStorage.get(key).map(_.lobsterID).map(MatcherInterface.deleteModel)
      octopusID
    }
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
    *
    * @param request POST request with octopus information
    * @return Case class object for JSON conversion
    */
  def updateOctopus(id: OctopusID, request: OctopusRequest): Octopus = {


    val SemanticTypeObject(classes, labelData, semanticTypeMap) = getSemanticTypes(request.ssds)

    // build the octopus from the request, adding defaults where necessary
    val octopusOpt = for {
      colMap <- Some(DatasetStorage.columnMap)
      original <- OctopusStorage.get(id)
      originalLobster <- ModelStorage.get(original.lobsterID)

      // update lobster
      lobsterRequest = createModelRequest(request, originalLobster, classes, labelData)
      updatedLobster = MatcherInterface.updateModel(original.lobsterID, lobsterRequest)

      // delete alignmentDir explicitly
      octopusDeletedAlignment <- OctopusStorage.deleteAlignmetDir(id)

      // build up the octopus request, and use defaults if not present...
      octopus <- Try {
        Octopus(
          id = id,
          lobsterID = original.lobsterID,
          description = request.description.getOrElse(original.description),
          name = request.name.getOrElse(original.name),
          modelingProps = request.modelingProps, // TODO: Remove option type!! in case it's None semantic-modeler uses default config for Karma; for now default config is available only in semantic-modeler
          ssds = request.ssds.getOrElse(original.ssds),
          ontologies = request.ontologies.getOrElse(original.ontologies),
          semanticTypeMap = semanticTypeMap,
          state = TrainState(Status.UNTRAINED, "", DateTime.now),
          dateCreated = original.dateCreated,
          dateModified = DateTime.now)
      }.toOption
      _ <- OctopusStorage.add(id, octopus)

    } yield {
      octopus
    }
    octopusOpt getOrElse {
      logger.error(s"Failed to update octopus $id.")
      throw InternalException("Failed to update octopus.")
    }
  }

  /**
    * Returns the public facing octopus from the storage layer
    *
    * @param id The octopus id
    * @return
    */
  def getOctopus(id: OctopusID): Option[Octopus] = {
    OctopusStorage.get(id)
  }

}
