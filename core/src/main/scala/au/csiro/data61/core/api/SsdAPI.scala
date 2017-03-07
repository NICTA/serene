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
package au.csiro.data61.core.api

import au.csiro.data61.core.drivers.SsdInterface
import au.csiro.data61.types.SsdTypes.SsdID
import au.csiro.data61.types._
import io.finch._
import io.finch.json4s.decodeJson
import shapeless.{:+:, CNil}

import scala.language.postfixOps
import scala.util.{Failure, Success}

/**
  * SSD REST endpoints...
  */
object SsdAPI extends RestAPI {
  protected val SsdRootPath = "ssd"

  /**
    * Lists keys of all SSDs.
    *
    * This endpoint handles GET requests for /version/ssd.
    */
  val listSsds: Endpoint[List[SsdID]] = get(APIVersion :: SsdRootPath) {
    Ok(SsdInterface.ssdKeys)
  }

  /**
    * Adds a new SSD as specified by the json body.
    *
    * {
    * SsdRequest
    * }
    *
    * Returns a JSON SSD object with id.
    *
    */
  val createSsd: Endpoint[Ssd] = post(APIVersion :: SsdRootPath :: jsonBody[SsdRequest]) {
    (request: SsdRequest) =>
      logger.info(s"Creating SSD with name=${request.name}.")
      SsdInterface.createSsd(request) match {
        case Success(ssd) => Ok(ssd)
        case Failure(th) =>
          logger.error(s"SSD with name=${request.name} could not be created.", th)
          InternalServerError(InternalException(s"SSD could not be created."))
      }
  }

  /**
    * Gets the SSD with specified ID.
    *
    * The endpoint handles GET requests for /version/ssd/:id.
    */
  val getSsd: Endpoint[Ssd] = get(APIVersion :: SsdRootPath :: int) { (id: Int) =>
    logger.info(s"Getting SSD with ID=$id")

    SsdInterface.getSsd(id) match {
      case Some(ssd) => Ok(ssd)
      case None => NotFound(NotFoundException(s"SSD $id not found"))
    }
  }

  /**
    * Updates the SSD with specified ID.
    *
    * This endpoint handles POST requests for /version/ssd/:id with an application/json body
    * containing the new SSD.
    */
  val updateSsd: Endpoint[Ssd] = post(APIVersion :: SsdRootPath :: int :: jsonBody[SsdRequest]) {
    (id: SsdID, request: SsdRequest) =>
      logger.info(s"Updating SSD with name=${request.name}.")

      SsdInterface.updateSsd(id, request) match {
        case Success(ssd) => Ok(ssd)
        case Failure(th) =>
          logger.error(s"SSD with name=${request.name} could not be updated.", th)
          InternalServerError(InternalException(s"SSD could not be updated."))
      }
  }

  /**
    * Deletes the SSD with specified ID.
    *
    * This endpoint handles DELETE requests for /version/ssd/:id.
    */
  val deleteSsd: Endpoint[Ssd] = delete(APIVersion :: SsdRootPath :: int) {
    (id: Int) =>
      logger.info(s"Deleting SSD with ID=$id")

      SsdInterface.deleteSsd(id) match {
        case Success(ssd) => Ok(ssd)
        case Failure(th) => InternalServerError(new RuntimeException(th))
      }
  }

  /**
    * Final endpoints for the Dataset endpoint...
    */
  val endpoints: Endpoint[List[SsdID] :+: Ssd :+: Ssd :+: Ssd :+: Ssd :+: CNil] =
    listSsds :+:
      createSsd :+:
      getSsd :+:
      updateSsd :+:
      deleteSsd
}


/**
  * SSDRequest is the user-facing object for creating and returning SSDs...
  * NOTE: columns and their transformations will not be user-provided now,
  * but rather automatically generated from mappings.
  *
  * @param name The name label used for the SSD
  * @param ontologies The list of Ontologies used in this ssd
  * @param semanticModel The semantic model used to describe how the columns map to the ontology
  * @param mappings The mappings from the attributes to the semantic model
  */
case class SsdRequest(name: String,
                      ontologies: List[Int], // Int=OwlID ==> we have to use Int due to JSON bug
                      semanticModel: Option[SemanticModel], // create = empty, returned = full
                      mappings: Option[SsdMapping])  // create = empty, returned = full

/**
  * Return type to user from the API when performing Octopus prediction
  * @param predictions Ordered list of predictions
  */
case class SsdResults(predictions: List[(SsdRequest, SemanticScores)])