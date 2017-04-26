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
import au.csiro.data61.types.Exceptions.TypeException
import au.csiro.data61.types.SsdTypes.{OwlID, SsdID}
import au.csiro.data61.types._
import com.typesafe.scalalogging.LazyLogging
import io.finch._
import io.finch.json4s.decodeJson
import org.joda.time.DateTime
import org.json4s.MappingException
import org.json4s.jackson.JsonMethods._

import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

/**
  * SSD REST endpoints...
  *
  * POST  :8080/v1.0/ssd/ <- SSDFrontEnd
  * GET   :8080/v1.0/ssd/{id} -> SemanticSourceDesc
  * PATCH :8080/v1.0/ssd/{id} <- SSDFrontEnd
  * DELETE :8080/v1.0/ssd/{id}
  *
  * POST :8080/v1.0/octopus/{id} <- OctopusRequest(list of SsdID)
  * POST :8080/v1.0/octopus/{id}/predict?datasetID={id} -> SsdResults(predictions = List[(SSDRequest, score)])
  * POST :8080/v1.0/octopus/{id}/train
  */
object SsdAPI extends RestAPI {
  protected val SsdRootPath = "ssd"

  val junkSSD = Ssd(
    id = 1,
    name = "test",
    attributes = List(
      SsdAttribute(1) // this is a dummy attribute created using the specified ColumnId
    ),
    ontologies = List(1, 2, 3),
    semanticModel = None,
    mappings = None,
    dateCreated = DateTime.now(),
    dateModified = DateTime.now()
  )

  /**
    * Returns all dataset keys
    *
    * curl http://localhost:8080/v1.0/ssd
    */
  val ssdRoot: Endpoint[List[Int]] = get(APIVersion :: SsdRootPath) {
    Ok(SsdInterface.storageKeys)
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
//  val ssdCreate: Endpoint[Ssd] = post(APIVersion :: SsdRootPath :: stringBody) {
//    (body: String) =>
//      logger.info(s"Attempting to create SSD...")
//      (for {
//        request <- parseSsdRequest(body)
//        attempt <- Try{ SsdInterface.createSsd(request) }
//      } yield attempt) match {
//        case Success(ssd) =>
//          Ok(ssd)
//        case Failure(err) =>
//          logger.error(s"SSD could not be created.", err)
//          err match {
//            case ex: BadRequestException => BadRequest(ex)
//            case _ => InternalServerError(InternalException("SSD could not be created."))
//          }
//      }
//  }
  val ssdCreate: Endpoint[Ssd] = post(APIVersion :: SsdRootPath :: jsonBody[SsdRequest]) {
    (request: SsdRequest) =>
      logger.info(s"Creating SSD with name=${request.name}.")
      Try{ SsdInterface.createSsd(request) } match {
        case Success(ssd) =>
          Ok(ssd)
        case Failure(err) =>
          logger.error(s"SSD with name=${request.name} could not be created.", err)
          err match {
            case ex: BadRequestException => BadRequest(ex)
            case _ => InternalServerError(InternalException("SSD could not be created."))
          }
      }
  } handle {
  case e: TypeException =>
    logger.error(s"TypeException ${e.getMessage}")
    BadRequest(e)
  case e: Exception =>
    logger.error(s"Parsing error ${e.getMessage}")
    BadRequest(BadRequestException(s"Request body cannot be parsed. Check SSD: ${e.getCause}, ${e.getMessage}"))
}

  /**
    * Returns a JSON SSD object at id
    *
    * curl http://localhost:8080/v1.0/ssd/12354687
    */
  val ssdGet: Endpoint[Ssd] = get(APIVersion :: SsdRootPath :: int) {
    (id: Int) =>

      logger.debug(s"Get ssd id=$id")

      val ssd = Try { SsdInterface.get(id) }

      ssd match {
        case Success(Some(s))  =>
          Ok(s)
        case Success(None) =>
          NotFound(NotFoundException(s"SSD $id does not exist."))
        case Failure(err) =>
          BadRequest(BadRequestException(err.getMessage))
      }
  }

  /**
    * Patch a portion of a SSD. Only description and typeMap
    *
    * Returns a JSON SSD object at id
    *
    * curl -X POST -d 'description=This is the new description'
    * http://localhost:8080/v1.0/ssd/12354687
    */
  val ssdPatch: Endpoint[Ssd] = post(APIVersion :: SsdRootPath :: int :: jsonBody[SsdRequest]) {
    (id: SsdID, request: SsdRequest) =>
      logger.info(s"Updating SSD with name=${request.name}.")
      SsdInterface.updateSsd(id, request) match {
        case Success(ssd) => Ok(ssd)
        case Failure(err) =>
          logger.error(s"SSD with name=${request.name} could not be updated.", err)
          err match {
            case ex: BadRequestException => BadRequest(ex)
            case ex: NotFoundException => NotFound(ex)
            case _ => InternalServerError(InternalException(s"SSD could not be updated."))
          }
      }
  }

  /**
    * Deletes the ssd at position id.
    *
    * curl -X DELETE http://localhost:8080/v1.0/ssd/12354687
    */
  val ssdDelete: Endpoint[String] = delete(APIVersion :: "ssd" :: int) {
    (id: Int) =>
      Try(SsdInterface.delete(id)) match {

        case Success(Some(_)) =>
          logger.debug(s"Deleted ssd $id")
          Ok(s"SSD $id deleted successfully.")

        case Success(None) =>
          logger.error(s"Could not find ssd $id")
          NotFound(NotFoundException(s"SSD $id could not be found"))

        case Failure(err: BadRequestException) =>
          BadRequest(err)

        case Failure(err: InternalException) =>
          InternalServerError(err)

        case Failure(err) =>
          logger.error(s"Some other problem with ssd $id deletion: ${err.getMessage}")
          InternalServerError(InternalException(s"Failed to delete ssd."))
      }
  }

  /**
    * Helper function to parse a string into a OctopusRequest object...
    *
    * @param str The json string with the octopus request information
    * @return
    */
  private def parseSsdRequest(str: String): Try[SsdRequest] = {

    for {
      raw <- Try { parse(str) }

      name <- HelperJSON.parseOption[String]("name", raw)

      ssds <- HelperJSON.parseOption[List[SsdID]]("ssds", raw)

      ontologies <- HelperJSON.parseOption[List[OwlID]]("ontologies", raw)

      semanticModel <- HelperJSON.parseOption[SemanticModel]("semanticModel", raw)

      mappings <- HelperJSON.parseOption[SsdMapping]("mappings", raw)

    } yield SsdRequest(
      name = name,
      ontologies = ontologies,
      semanticModel = semanticModel,
      mappings = mappings
    )
  }

  /**
    * Final endpoints for the Ssd endpoint...
    */
  val endpoints =
    ssdRoot :+:
      ssdCreate :+:
      ssdGet :+:
      ssdPatch :+:
      ssdDelete
}


/**
  * SSDRequest is the user-facing object for creating and returning SSDs...
  * NOTE: columns and their transformations will not be user-provided now,
  * but rather automatically generated from mappings.
  *
  * create = empty, returned = full
  *
  * @param name The name label used for the SSD
  * @param ontologies The list of Ontologies used in this ssd
  * @param semanticModel The semantic model used to describe how the columns map to the ontology
  * @param mappings The mappings from the attributes to the semantic model
  */
case class SsdRequest(
    name: Option[String],
    ontologies: Option[List[Int]],
    semanticModel: Option[SemanticModel],
    mappings: Option[SsdMapping]) extends LazyLogging {
  /**
    * Convert SsdRequest to Ssd
    *
    * @param ssdID Id to be used for the generated Ssd
    * @return
    */
  def toSsd(ssdID: SsdID): Try[Ssd] = Try {
    logger.debug(s"Converting SsdRequest $name to Ssd...")
    // get a list of attributes from the mappings
    // NOTE: for now we automatically generate them to be equal to the original columns
    val ssdAttributes: List[SsdAttribute] = mappings
      .map(_.mappings.keys.toList)
      .getOrElse(List.empty[Int])
      .map(SsdAttribute(_))

    val now = DateTime.now

    Ssd(ssdID,
      name = name.getOrElse(""),
      attributes = ssdAttributes,
      ontologies = ontologies.getOrElse(List.empty[OwlID]),
      semanticModel = semanticModel,
      mappings = mappings,
      dateCreated = now,
      dateModified = now
    )
  }
}

/**
  * Return type to user from the API when performing Octopus prediction
 *
  * @param predictions Ordered list of predictions
  */
case class SsdResults(predictions: List[SsdResult])

case class SsdResult(ssd: SsdRequest, score: SemanticScores)

