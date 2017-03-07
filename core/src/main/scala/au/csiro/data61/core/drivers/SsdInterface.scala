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

import java.io.IOException

import au.csiro.data61.core.api.{InternalException, SsdRequest}
import au.csiro.data61.core.drivers.Generic.genID
import au.csiro.data61.core.storage.{DatasetStorage, SsdStorage}
import au.csiro.data61.types.SsdTypes.SsdID
import au.csiro.data61.types.{Ssd, SsdAttribute}
import com.typesafe.scalalogging.LazyLogging
import org.joda.time.DateTime

import scala.util.Try

object SsdInterface extends StorageInterface[SsdID, Ssd] with LazyLogging {

  override val storage = SsdStorage

  protected def missingReferences[Ssd](resource: Ssd): StorageDependencyMap = Map()

  protected def dependents[Ssd](resource: Ssd): StorageDependencyMap = Map()

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
      throw InternalException(msg)
    }

    // check that ontologies are available in the OwlStorage
    if (!request.ontologies.forall(OwlInterface.owlKeys.toSet.contains)) {
      val msg = "SSD cannot be added to the storage: ontologies do not exist in the OwlStorage."
      logger.error(msg)
      throw InternalException(msg)
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
      throw InternalException(msg)
    }

    ssd
  }

  /**
    * Wraps check and conversion procedures.
    * @param request The SSD request.
    * @param id The SSD ID.
    * @return Valid SSD if successful. Otherwise the exception that caused the failure.
    */
  protected def checkAndConvertSsdRequest(request: SsdRequest, id: SsdID): Try[Ssd] = Try {
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
    // we convert here the request to the SSD and also check if the SSD is complete
    convertSsdRequest(request, ssdAttributes, id)
  }

  /**
    * Creates an SSD.
    * @param request The SSD creation request.
    * @return The created SSD if successful. Otherwise the exception that caused the failure.
    */
  def createSsd(request: SsdRequest): Try[Ssd] = {
    val id = genID

    checkAndConvertSsdRequest(request, id) flatMap { (ssd: Ssd) => Try {
      add(ssd) match {
        case Some(_) => ssd
        case None => throw new IOException(s"SSD $id could not be created.")
      }
    }}
  }

  /**
    * Updates an SSD.
    * @param id The SSD ID.
    * @param request The SSD update request.
    * @return The updated SSD if successful. Otherwise the exception that caused the failure.
    */
  def updateSsd(id: SsdID, request: SsdRequest): Try[Ssd] = Try {
    storage.get(id) match {
      case Some(originalSsd) =>
        checkAndConvertSsdRequest(request, id)
          .flatMap { (ssd: Ssd) => Try {
            val updatedSsd = ssd.copy(dateCreated = originalSsd.dateCreated)
            update(updatedSsd) match {
              case Some(_) => updatedSsd
              case None => throw new IOException(s"SSD $id could not be updated.")
            }
          }}
          .get
      case None =>
        throw new NoSuchElementException(s"SSD $id not found.")
    }
  }

  /**
    * Gets the IDs of available SSDs.
    *
    * @return The list of SSD IDs.
    */
  def ssdKeys: List[SsdID] = {
    storage.keys
  }

  /**
    * Deletes an SSD.
    * @param id The SSD ID.
    * @return The deleted SSD if successful. Otherwise the exception that caused the failure.
    */
  def deleteSsd(id: SsdID): Try[Ssd] = Try {
    storage.get(id) match {
      case Some(ssd) =>
        remove(ssd) match {
          case Some(_) => ssd
          case None =>
            throw new IOException(s"SSD $id could not be deleted.")
        }
      case None =>
        throw new NoSuchElementException(s"SSD $id not found.")
    }
  }

  /**
    * Gets an SSD.
    *
    * @param id The ID of the SSD.
    * @return The SSD if found.
    */
  def getSsd(id: SsdID): Option[Ssd] = storage.get(id)
}
