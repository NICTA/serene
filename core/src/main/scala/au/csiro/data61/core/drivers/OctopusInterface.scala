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

import java.nio.file.Path

import au.csiro.data61.core.api.{BadRequestException, InternalException, ModelRequest, OctopusRequest}
import au.csiro.data61.core.storage._
import au.csiro.data61.modeler.{PredictOctopus, TrainOctopus}
import au.csiro.data61.types._
import au.csiro.data61.types.DataSetTypes._
import au.csiro.data61.types.ModelTypes.{Status, TrainState, _}
import au.csiro.data61.types.SSDTypes._
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
object OctopusInterface extends LazyLogging{

  val MissingValue = "unknown"
  val defaultNumSemanticTypes = 4


  /**
    * Build a new Octopus from the request.
    * We also build the associated schema matcher Model.
    *
    * @param request The request object from the API
    * @return
    */
  def createOctopus(request: OctopusRequest): Octopus = {

    val id = genID

    val (classes, labelData, semanticTypeMap) = getSemanticTypes(request.ssds)

    // we need first to create the schema matcher model
    val modelReq = ModelRequest(description = request.description,
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
      case Success(model) =>  model.id
      case Failure(err) =>
        throw InternalException(s"Octopus cannot be created since lobster fled: $err")
    }

    // build the octopus from the request, adding defaults where necessary
    val octopusOpt = for {

      octopus <- Try {
        Octopus(id,
          ontologies = getOntologies(request.ssds, request.ontologies),
          ssds = request.ssds.getOrElse(List.empty[Int]),
          lobsterID = lobsterID,
          modelingProps = request.modelingProps,
          alignmentDir = None,
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

    for {
      octopus <- OctopusStorage.get(id)
      model <- ModelStorage.get(octopus.lobsterID)
      octopusState = octopus.state
      newState = octopusState.status match {
        case Status.COMPLETE if OctopusStorage.isConsistent(id) && !force =>
          logger.info(s"Octopus $id is already trained.")
          octopusState
        case Status.BUSY =>
          // if it is complete or pending, just return the value
          logger.info(s"Octopus $id is busy.")
          octopusState
        case Status.COMPLETE | Status.UNTRAINED | Status.ERROR =>
          // in the background we launch the training...
          logger.info("Launching training.....")
          // first we set the model state to training....
          val newState = OctopusStorage.updateTrainState(id, Status.BUSY)

          // train the schema matcher model
          MatcherInterface.trainModel(OctopusStorage.get(id).get.lobsterID)
          // launch training for the octopus
          launchOctopusTraining(id)

          newState.get
        case _ =>
          octopusState
      }
    } yield newState

  }

  /**
    * Asynchronously launch the training process, and write
    * to storage once complete. The actual state will be
    * returned from the above case when re-read from the
    * storage layer.
    *
    * @param id Octopus id for which training will be launched
    */
  private def launchOctopusTraining(id: OctopusID)(implicit ec: ExecutionContext): Unit = {

    Future {
      val octopus = OctopusStorage.get(id).get
      // get SSDs for the training
      val knownSSDs: List[SemanticSourceDesc] = octopus.ssds.flatMap(SSDStorage.get)
      // get location strings of the ontologies
      val ontologies: List[String] = octopus.ontologies.flatMap(OwlStorage.get).map(_.path.toString)
      // proceed with training...
      TrainOctopus.train(octopus, OctopusStorage.getAlignmentDirPath(id), ontologies, knownSSDs)
    } onComplete {
      case Success(path) =>
        // we update the status, the state date and do not delete the model.rf file
        OctopusStorage.updateTrainState(id, Status.COMPLETE, "", Some(path))
      case Failure(err) =>
        // we update the status, the state date and delete the model.rf file
        val msg = s"Failed to train octopus $id: ${err.getMessage}."
        logger.error(msg)
        // TODO: delete alignmentDir explicitly?
        OctopusStorage.updateTrainState(id, Status.ERROR, msg, None)
    }
  }

  /**
    * Perform prediction using the model
    *
    * @param id The model id
    * @param ssdID id of the SSD
    * @return
    */
  def predictOctopus(id: OctopusID, ssdID : SsdID): SSDPrediction = {

    if (OctopusStorage.isConsistent(id)) {
      // do prediction
      logger.info(s"Launching prediction for OCTOPUS $id...")
      val octopus: Octopus = OctopusStorage.get(id).get

      // we get here attributes which are transformed columns
      val ssdColumns = SSDStorage.get(ssdID).get.attributes.map(_.id)
      val datasets: List[DataSetID] =
        DatasetStorage.columnMap.filterKeys(ssdColumns.contains).map(_._2.datasetID).toList.distinct

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
      val attrToColMap: Map[Int,Int] = SSDStorage.get(ssdID).get.attributes.map(x =>(x.id,x.id)).toMap

      PredictOctopus.predict(octopus,
        octopus.ontologies.flatMap(OwlStorage.get).map(_.path.toString),
        SSDStorage.get(ssdID).get,
        dsPredictions,
        attrToColMap,
        defaultNumSemanticTypes)
        .getOrElse(throw InternalException(s"No SSD predictions are available for octopus $id."))

    } else {
      val msg = s"Prediction failed. Octopus $id is not trained."
      // prediction is impossible since the model has not been trained properly
      logger.warn(msg)
      throw BadRequestException(msg)
    }
  }

  /**
    * Generate a random positive integer id
    *
    * @return Returns a random positive integer
    */
  protected def genID: Int = Random.nextInt(Integer.MAX_VALUE)

  /**
    * Split URI into the namespace and the name of the resource (either class, or property)
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
  protected def getSemanticTypes(ssds: Option[List[Int]]
                                ): (Option[List[String]], Option[Map[Int, String]], Option[Map[String,String]]) = {

    logger.debug("Getting semantic type info from the octopus...")
    val givenSSDs = ssds.getOrElse(List.empty[Int])

    if (givenSSDs.isEmpty) {
      // everything is empty if there are no SSDs
      (None, None, None)
    }
    else {
      // here we want to get a map from AttrID to (URI of class node, URI of data node)
      // TODO: the mapping can be to the ClassNode
      val ssdMaps: List[(AttrID,(String,String))] = givenSSDs
        .flatMap(SSDStorage.get)
        .filter(_.mappings.isDefined)
        .filter(_.semanticModel.isDefined)
        .flatMap { ssd: SemanticSourceDesc =>
          ssd.mappings.get.mappings.map { // SSDs contain SSDMapping which is a mapping AttrID --> NodeID
            case (attrID, nodeID) => (attrID,
              ssd.semanticModel.get
                .getDomainType(nodeID) // FIXME: what if the mapping is to the class node and not the data node?!
                .getOrElse(throw InternalException("Semantic Source Description is not proper, problems with " +
                  "mappings or semantic model")))
            } toList
        }

      val semanticTypeMap: Map[String, String] = ssdMaps.flatMap {
        // TODO: what if we have the same labels with different namespaces? Only one will be picked here
        case (attrID, (classURI, propURI)) => List( splitURI(classURI), splitURI(propURI))
      }.toMap

      // semantic type (aka class label) is constructed as "the label of class URI"---"the label of data property URI"
      // TODO: what if the column is mapped to the class node?
      val labelData: Map[Int, String] = ssdMaps.map {
        case (attrID: Int, (classURI, propURI)) => (attrID, constructLabel(classURI,propURI))
      } toMap

      val classes: List[String] = labelData.map {
        case (attrID, semType) => semType
      }.toList.distinct

      (Some(classes), Some(labelData), Some(semanticTypeMap))
    }
  }

  /**
    * Construct the semantic type (aka class label) as "the label of class URI"---"the label of data property URI"
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
    * @param ssds List of ids of the semantic source descriptions.
    * @param ontologies List of ids of the ontologies which need to be additionally included into the Octopus.
    * @return
    */
  protected def getOntologies(ssds: Option[List[Int]],
                              ontologies: Option[List[Int]]): List[Int] = {
    logger.debug("Getting owls for octopus")
    val ssdOntologies: List[Int] = ssds.getOrElse(List.empty[Int])
      .flatMap(SSDStorage.get)
      .flatMap(_.ontology)

    ssdOntologies ++ ontologies.getOrElse(List.empty[Int])
  }
}
