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

import au.csiro.data61.core.api.{BadRequestException, InternalException, NotFoundException, SsdRequest}
import au.csiro.data61.core.drivers.Generic._
import au.csiro.data61.core.storage.{DatasetStorage, OctopusStorage, OwlStorage, SsdStorage}
import au.csiro.data61.types.ColumnTypes.ColumnID
import au.csiro.data61.types.Ssd
import au.csiro.data61.types.SsdTypes._
import com.typesafe.scalalogging.LazyLogging

import scala.util.{Failure, Success, Try}

object SsdInterface extends StorageInterface[SsdKey, Ssd] with LazyLogging {

  override val storage = SsdStorage

  protected def missingReferences(resource: Ssd): StorageDependencyMap = {

    val presentColIds: List[ColumnID] = resource.attributes.map(_.id)

    // missing columns
    val colIDs: List[ColumnID] = presentColIds.filterNot(DatasetStorage.columnMap.keys.toSet.contains)

    // missing owls
    val owlIDs: List[OwlID] = resource.ontologies.filterNot(OwlStorage.keys.toSet.contains)

    StorageDependencyMap(owl = owlIDs, column = colIDs)
  }

  protected def dependents(resource: Ssd): StorageDependencyMap = {
    // only octopi
    val octoRefIds: List[OctopusID] = OctopusStorage.keys
      .flatMap(OctopusStorage.get)
      .map(x => (x.id, x.ssds.toSet))
      .filter(_._2.contains(resource.id))
      .map(_._1)

    StorageDependencyMap(octopus = octoRefIds)
  }

  /**
    * Check the request and if it's ok add a corresponding SSD to the storage.
    *
    * @param request Request object
    * @return
    */
  def createSsd(request: SsdRequest): Ssd = {
    val id = genID

    request.toSsd(id) match {
      case Success(ssd) =>
        // check if the SSD is consistent and complete
        if (!ssd.isComplete) {
          val msg = "SSD cannot be added to the storage: it is not connected. Check semanticModel and mappings."
          logger.error(msg)
          throw BadRequestException(msg)
        }

        add(ssd) match {
          case Some(key: SsdID) =>
            ssd
          case _ =>
            throw InternalException("Failed to create resource.")
        }
      case Failure(ex) =>
        val msg = "SSD cannot be added to the storage: it is invalid."
        logger.error(msg, ex)
        throw BadRequestException(msg)
    }
  }

  /**
    * Updates an SSD.
 *
    * @param id The SSD ID.
    * @param request The SSD update request.
    * @return The updated SSD if successful. Otherwise the exception that caused the failure.
    */
  def updateSsd(id: SsdID, request: SsdRequest): Try[Ssd] = Try {
    storage.get(id) match {
      case Some(originalSsd) =>
        val checkedRequest = SsdRequest(
          name = request.name.orElse(Option(originalSsd.name)),
          ontologies = request.ontologies.orElse(Option(originalSsd.ontologies)),
          semanticModel = request.semanticModel.orElse(originalSsd.semanticModel),
          mappings = request.mappings.orElse(originalSsd.mappings)
        )

        checkedRequest.toSsd(id) match {
          case Success(ssd) =>
            val updatedSsd = ssd.copy(dateCreated = originalSsd.dateCreated)

            // check if the SSD is consistent and complete
            if (!updatedSsd.isComplete) {
              val msg = "SSD cannot be updated to the storage: it is not connected."
              logger.error(msg)
              throw BadRequestException(msg)
            }

            update(updatedSsd) match {
              case Some(_) => updatedSsd
              case None => throw InternalException(s"SSD $id could not be updated.")
            }
          case Failure(ex) =>
            val msg = "SSD cannot be updated to the storage: it is invalid."
            logger.error(msg, ex)
            throw BadRequestException(msg)
        }
      case None =>
        throw NotFoundException(s"SSD $id not found.")
    }
  }
}
