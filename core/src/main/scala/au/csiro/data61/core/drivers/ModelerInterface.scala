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

import au.csiro.data61.core.api.{ModelRequest, BadRequestException, InternalException, OctopusRequest}
import au.csiro.data61.core.drivers.MatcherInterface._
import au.csiro.data61.core.storage.{SsdStorage, OctopusStorage, DatasetStorage, OwlStorage}
import au.csiro.data61.core.types.ModelerTypes.OwlDocumentFormat.OwlDocumentFormat
import au.csiro.data61.core.types.DataSetTypes._
import au.csiro.data61.core.types.Training.{Status, TrainState}
import au.csiro.data61.core.types._
import au.csiro.data61.core.types.ModelerTypes.{SsdID, Octopus, OctopusID, Owl, OwlID}
import com.typesafe.scalalogging.LazyLogging
import org.joda.time.DateTime

import scala.util.{Random, Try}

import language.postfixOps

object ModelerInterface extends LazyLogging {

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
    * createOctopus builds a new Octopus object from a OctopusRequest
    *
    * @param request The request object from the API
    * @return
    */
  def createOctopus(request: OctopusRequest): Octopus = {

    val id = Generic.genID
    //val dataRef = validKeys(request.labelData)

    // build the octopus from the request, adding defaults where necessary
    val octopusOpt = for {
      colMap <- Some(DatasetStorage.columnMap)

      modelID <- Try { MatcherInterface.createModel(ModelRequest(
        description = request.description,
        modelType = None,
        classes = None,
        features = None,
        costMatrix = None,
        labelData = None,
        resamplingStrategy = None,
        numBags = None,
        bagSize = None
      ))} map { _.id } toOption

      // build up the octopus request, and use defaults if not present...
      octopus <- Try {
        Octopus(
          id = id,
          modelID = modelID,
          description = request.description.getOrElse(MissingValue),
          name = request.name.getOrElse(MissingValue),
          ssds = request.ssds.getOrElse(List.empty[Int]),
          ontologies = List(1), // TODO: add real ontologies!!!!
          state = TrainState(Status.UNTRAINED, "", DateTime.now),
          dateCreated = DateTime.now,
          dateModified = DateTime.now)
      }.toOption
      _ <- OctopusStorage.add(id, octopus)

    } yield {
      octopus
    }
    octopusOpt getOrElse { throw InternalException("Failed to create resource.") }
  }

  /**
    * Deletes the octopus
    *
    * @param key Key for the octopus
    * @return
    */
  def deleteOctopus(key: OctopusID): Option[OctopusID] = {
    OctopusStorage.remove(key)
  }

  /**
    * Parses a octopus request to construct a octopus object
    * for updating. The index is searched for in the database,
    * and if update is successful, returns the case class response
    * object.
    *
    * @param request POST request with octopus information
    * @return Case class object for JSON conversion
    */
  def updateOctopus(id: OctopusID, request: OctopusRequest): Octopus = {

    // build the octopus from the request, adding defaults where necessary
    val octopusOpt = for {
      colMap <- Some(DatasetStorage.columnMap)
      original <- OctopusStorage.get(id)
      // build up the octopus request, and use defaults if not present...
      octopus <- Try {
        Octopus(
          id = id,
          modelID = original.modelID,
          description = request.description.getOrElse(original.description),
          name = request.name.getOrElse(original.name),
          ssds = request.ssds.getOrElse(original.ssds),
          ontologies = List(1), // TODO: add real ontologies!!!!
          state = TrainState(Status.UNTRAINED, "", DateTime.now),
          dateCreated = original.dateCreated,
          dateModified = DateTime.now)
      }.toOption
      _ <- OctopusStorage.add(id, octopus)

    } yield {
      octopus
    }
    octopusOpt getOrElse { throw InternalException("Failed to create resource.") }
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

  /**
    * Trains the octopus
    *
    * @param id The octopus id
    * @return
    */
  def trainOctopus(id: OctopusID, force: Boolean = false): Option[TrainState] = {

    for {
      octopus <- OctopusStorage.get(id)
      state = octopus.state
      newState = state.status match {
        case Status.COMPLETE if OctopusStorage.isConsistent(id) && !force =>
          logger.info(s"Octopus $id is already trained.")
          state
        case Status.BUSY =>
          // if it is complete or pending, just return the value
          logger.info(s"Octopus $id is busy.")
          state
        case Status.COMPLETE | Status.UNTRAINED | Status.ERROR =>
          // in the background we launch the training...
          logger.info("Launching training.....")
          // first we set the octopus state to training....
          val newState = OctopusStorage.updateTrainState(id, Status.BUSY)
          launchTraining(id)
          newState.get
        case _ =>
          state
      }
    } yield newState

  }

  /**
    * Asynchronously launch the training process, and write
    * to storage once complete. The actual state will be
    * returned from the above case when re-read from the
    * storage layer.
    *
    * @param id Octopus key for the octopus to be launched
    */
  private def launchTraining(id: OctopusID): Unit = {

  }

  /**
    * Perform prediction using the octopus
    *
    * @param id The octopus id
    * @param datasetID Optional id of the dataset
    * @return
    */
  def predictOctopus(id: OctopusID, datasetID : DataSetID): DataSetPrediction = {

    if (OctopusStorage.isConsistent(id)) {
      // do prediction
      logger.info(s"Launching prediction for octopus $id...")
      //OctopusPredictor.predict(id, datasetID)

      // TODO: make this real!!!
      DataSetPrediction(1, 1, Map.empty[String, ColumnPrediction])
    } else {
      val msg = s"Prediction failed. Octopus $id is not trained."
      // prediction is impossible since the octopus has not been trained properly
      logger.warn(msg)
      throw BadRequestException(msg)
    }
  }


  /**
    * Creates an OWL document with related information.
    *
    * @param description The description of the OWL document.
    * @param format The format of the OWL document.
    * @param inputStream The input stream of the OWL document.
    * @return The created OWL information if successful. Otherwise the exception that caused the
    *         failure.
    */
  def createOwl(
      name: String,
      description: String,
      format: OwlDocumentFormat,
      inputStream: InputStream): Try[Owl] = Try {
    val id = Random.nextInt(Integer.MAX_VALUE)
    val now = DateTime.now
    val owl = Owl(
      id = id,
      name = name,
      format = format,
      description = description,
      dateCreated = now,
      dateModified = now
    )

    OwlStorage.add(id, owl) match {
      case Some(_) =>
        OwlStorage.writeOwlDocument(OwlStorage.getOwlDocumentPath(id, format), inputStream).get
        owl
      case None => throw new IOException(s"Owl $id could not be created.")
    }
  }

  /**
    * Gets the IDs of available OWL documents.
    *
    * @return The list of OWL IDs.
    */
  def owlKeys: List[OwlID] = OwlStorage.keys

  /**
    * Gets information about an OWL document.
    *
    * @param id The ID of the OWL document.
    * @return Information about the OWL document if found.
    */
  def getOwl(id: OwlID): Option[Owl] = OwlStorage.get(id)

  /**
    * Updates information about an OWL document.
    *
    * @param id The ID of the OWL document.
    * @param description The description of the OWL document.
    * @return Updated information of the OWL document if successful. Otherwise the exception that
    *         caused the failure.
    */
  def updateOwl(id: OwlID, description: Option[String]): Try[Owl] = Try {
    OwlStorage.get(id) match {
      case Some(owl) =>
        val updatedOwl = owl.copy(
          description = description.getOrElse(owl.description)
        )
        OwlStorage.update(id, updatedOwl) match {
          case Some(_) => updatedOwl
          case None => throw new IOException(s"Owl $id could not be updated.")
        }
      case None => throw new NoSuchElementException(s"Owl $id not found.")
    }
  }

  /**
    * Deletes an OWL document.
    *
    * @param id The ID of the OWL document.
    * @return Information about the deleted OWL document if successful. Otherwise the exception that
    *         caused the failure.
    */
  def deleteOwl(id: OwlID): Try[Owl] = Try {
    OwlStorage.get(id) match {
      case Some(owl) =>
        OwlStorage.remove(id) match {
          case Some(_) => owl
          case None => throw new IOException(s"Owl $id could not be deleted.")
        }
      case None =>
        throw new NoSuchElementException(s"Owl $id not found.")
    }
  }
}
