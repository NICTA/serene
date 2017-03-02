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
object SsdInterface extends LazyLogging {

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
    if (!request.ontologies.forall(owlKeys.toSet.contains)) {
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
    * Creates an OWL document with related information.
    *
    * @param description The description of the OWL document.
    * @param format The format of the OWL document.
    * @param inputStream The input stream of the OWL document.
    * @return The created OWL information if successful. Otherwise the exception that caused the
    *         failure.
    */
  def createOwl(name: String,
                description: String,
                format: OwlDocumentFormat,
                inputStream: InputStream): Option[Owl] = {
    val id = genID
    val now = DateTime.now
    val owl = Owl(
      id = id,
      name = name,
      format = format,
      description = description,
      dateCreated = now,
      dateModified = now
    )

    for {
      owlId <- OwlStorage.add(id, owl)
      owlPath <- OwlStorage.writeOwlDocument(id, inputStream)
      curOwl <- OwlStorage.get(owlId)
    } yield curOwl

    //    OwlStorage.add(id, owl) match {
    //      case Some(_) =>
    //          OwlStorage.writeOwlDocument(id, inputStream)
    //      case None =>
    //        throw new IOException(s"Owl $id could not be created.")
    //    }
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
    * Gets the original OWL file uploaded to the server.
    *
    * @param owl The owl object
    * @return A buffered reader object
    */
  def getOwlDocument(owl: Owl): Try[Reader] = Try {
    Reader.fromFile(OwlStorage.getOwlDocumentPath(owl.id).map(_.toFile).get)
  }

  /**
    * Updates information about an OWL document.
    *
    * @param id The ID of the OWL document.
    * @param description The description of the OWL document.
    * @return Updated information of the OWL document if successful. Otherwise the exception that
    *         caused the failure.
    */
  def updateOwl(id: OwlID, description: Option[String]): Try[Owl] = Try {
    // TODO: check SsdStorage, OctopusStorage
    OwlStorage.get(id) match {
      case Some(owl) =>
        val updatedOwl = owl.copy(
          description = description.getOrElse(owl.description)
        )
        OwlStorage.update(id, updatedOwl) match {
          case Some(_) =>
            updatedOwl
          case None =>
            logger.error(s"Owl $id could not be updated.")
            throw InternalException(s"Owl $id could not be updated.")
        }
      case None =>
        throw NotFoundException(s"Owl $id not found.")
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

    if (OwlStorage.hasDependents(id)) {
      val msg = s"Owl $id cannot be deleted since it has dependents." +
        s"Delete first dependents."
      logger.error(msg)
      throw BadRequestException(msg)
    }

    // TODO: check SsdStorage, OctopusStorage
    OwlStorage.get(id) match {
      case Some(owl) =>
        OwlStorage.remove(id) match {
          case Some(_) =>
            owl
          case None =>
            throw InternalException(s"Owl $id could not be deleted.")
        }
      case None =>
        throw NotFoundException(s"Owl $id not found.")
    }
  }

  /**
    * Passes the ssd keys up to the API
    *
    * @return
    */
  def ssdKeys: List[SsdID] = {
    SsdStorage.keys
  }
}
