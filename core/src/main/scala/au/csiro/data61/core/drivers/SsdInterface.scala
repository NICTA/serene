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
import au.csiro.data61.core.storage.{DatasetStorage, OctopusStorage, OwlStorage, SsdStorage}
import au.csiro.data61.types.ColumnTypes.ColumnID
import au.csiro.data61.types.{Ssd, SsdAttribute}
import au.csiro.data61.types.SsdTypes._
import au.csiro.data61.core.drivers.Generic._
import com.typesafe.scalalogging.LazyLogging
import org.joda.time.DateTime

import scala.util.Try

object SsdInterface extends StorageInterface[SsdKey, Ssd] with LazyLogging {

  override val storage = SsdStorage

  protected def missingReferences(resource: Ssd): StorageDependencyMap = {

    val presentColIds: List[ColumnID] = resource.attributes.map(_.id)

    // missing columns
    val colIDs: List[ColumnID] = presentColIds.filterNot(DatasetStorage.columnMap.keys.toSet.contains)

    // missing owls
    val owlIDs: List[OwlID] = resource.ontology.filterNot(OwlStorage.keys.toSet.contains)

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
    * Check if the SsdRequest has proper parameters.
    * Then convert it to Semantic Source Description
    *
    * @param request SsdRequest coming from the API
    * @param ssdID id of the SSD
    */
  protected def convertSsdRequest(request: SsdRequest,
                                  ssdID: SsdID)
  : Ssd = {

    // get list of attributes from the mappings
    // NOTE: for now we automatically generate them to be equal to the original columns
    val ssdAttributes: List[SsdAttribute] = request
      .mappings
      .map(_.mappings.keys.toList)
      .getOrElse(List.empty[Int])
      .map(SsdAttribute(_))

    Ssd(ssdID,
      name = request.name,
      attributes = ssdAttributes,
      ontology = request.ontologies,
      semanticModel = request.semanticModel,
      mappings = request.mappings,
      dateCreated = DateTime.now,
      dateModified = DateTime.now
    )
  }

  /**
    * Check the request and if it's ok add a corresponding SSD to the storage.
    *
    * @param request Request object
    * @return
    */
  def createSsd(request: SsdRequest): Ssd = {

    val id = genID

    val ssd = convertSsdRequest(request, id)

    // check if the SSD is consistent and complete
    if (!ssd.isComplete){
      val msg = "SSD cannot be added to the storage: it is not complete."
      logger.error(msg)
      throw BadRequestException(msg)
    }

    add(ssd) match {
      case Some(key: SsdID) =>
        ssd
      case _ =>
        throw InternalException("Failed to create resource.")
    }
  }

  /**
    * Updates an SSD.
    * @param id The SSD ID.
    * @param request The SSD update request.
    * @return The updated SSD if successful. Otherwise the exception that caused the failure.
    */
  def updateSsd(id: SsdID, request: SsdRequest): Try[Ssd] = Try {
    // FIXME: we need to create new SsdRequest which either have values from the new request and otherwise from the old one
    storage.get(id) match {
      case Some(originalSsd) =>
        Try { convertSsdRequest(request, id) }
          .flatMap {
            (ssd: Ssd) => Try {
              val updatedSsd = ssd.copy(dateCreated = originalSsd.dateCreated)
              update(updatedSsd) match {
                case Some(_) => updatedSsd
                case None => throw InternalException(s"SSD $id could not be updated.")
              }
            }
          }
          .get
      case None =>
        throw NotFoundException(s"SSD $id not found.")
    }
  }


}
