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

import java.nio.file.Path

import au.csiro.data61.core.drivers.OctopusInterface
import au.csiro.data61.types._
import au.csiro.data61.types.SsdTypes.{SsdID, OwlID, Octopus, OctopusID}
import au.csiro.data61.types.Training.{Status, TrainState}
import io.finch._

import scala.language.postfixOps
import io.finch._
import org.joda.time.DateTime
import org.json4s.JValue
import org.json4s.JsonAST.JNothing
import org.json4s.jackson.JsonMethods._

import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

/**
  * Octopus REST endpoints...
  *
  * GET    /v1.0/octopus              -- json list of octopus ids
  * POST   /v1.0/octopus              -- json octopus object
  * GET    /v1.0/octopus/:id          -- json octopus object
  * GET    /v1.0/octopus/:id/train    -- returns async status obj
  * GET    /v1.0/octopus/:id/predict  -- returns async status obj
  * POST   /v1.0/octopus/:id          -- update
  * DELETE /v1.0/octopus/:id
  *
  * UPDATED:
  * POST :8080/v1.0/octopus/{id} <- OctopusRequest(list of SsdID)
  * POST :8080/v1.0/octopus/{id}/predict?datasetID={id} -> SsdResults(predictions = List[(SSDRequest, score)])
  * POST :8080/v1.0/octopus/{id}/train
  */
object OctopusAPI extends RestAPI {

  val TestOctopus = Octopus(
    id = 0,
    name = "test",
    lobsterID = 0,
    description = "This is an octopus description",
    ssds = List(0, 1, 2, 3),
    ontologies = List(),
    semanticTypeMap = Map.empty[String,String],
    modelingProps = ModelingProperties(),
    dateCreated = DateTime.now(),
    dateModified = DateTime.now(),
    state = TrainState(Status.COMPLETE, "ok", DateTime.now())
  )

  /**
    * Returns all octopus ids
    *
    * curl http://localhost:8080/v1.0/octopus
    */
  val octopusRoot: Endpoint[List[OctopusID]] = get(APIVersion :: "octopus") {
    Ok(OctopusInterface.storageKeys)
  }

  /**
    * Adds a new octopus as specified by the json body.
    * {
    *   "name": "hello",
    *   "description": "Testing octopus used for identifying phone numbers only.",
    *   "ssds": [1, 2, 3],
    *   "ontologies": [1, 2, 3],
    *   "modelingProps": "not implemented for now",
    *   "modelType": "randomForest",
    *   "features": ["isAlpha", "alphaRatio", "atSigns", ...],
    *   "resamplingStrategy": "ResampleToMean",
    *   "numBags": 10,
    *   "bagSize": 10
    *
    * }
    *
    * Returns a JSON octopus object with id.
    *
    */
  val octopusCreate: Endpoint[Octopus] = post(APIVersion :: "octopus" :: stringBody) {
    (body: String) =>
      (for {
        request <- parseOctopusRequest(body)
        _ <- Try {
          request.ssds match {
            case Some(x) if x.nonEmpty =>
              request
            case _ =>
              logger.error("No Semantic Source Descriptions found.")
              throw BadRequestException("No Semantic Source Descriptions found.")
          }
        }

        m <- OctopusInterface.createOctopus(request)
      } yield m)
      match {
        case Success(mod) =>
          Ok(mod)
        case Failure(err: BadRequestException) =>
          BadRequest(err)
        case Failure(err: InternalException) =>
          InternalServerError(err)
        case Failure(err) =>
          InternalServerError(InternalException(err.getMessage))
      }
  }

  /**
    * Returns a JSON object at id
    */
  val octopusGet: Endpoint[Octopus] = get(APIVersion :: "octopus" :: int) {
    (id: Int) =>
      Try { OctopusInterface.get(id) } match {
        case Success(Some(ds))  =>
          Ok(ds)
        case Success(None) =>
          NotFound(NotFoundException(s"Octopus $id does not exist."))
        case Failure(err) =>
          BadRequest(BadRequestException(err.getMessage))
      }
  }

  /**
    * Trains a octopus at id
    * If training has been successfully launched, it returns nothing
    */
  val octopusTrain: Endpoint[Unit] = post(APIVersion :: "octopus" :: int :: "train" :: paramOption("force")) {
    (id: Int, force: Option[String]) =>
      Try {
        OctopusInterface.trainOctopus(id, force.exists(_.toBoolean))
      } match {
        case Success(Some(_))  =>
          Accepted[Unit]
        case Success(None) =>
          logger.error("Unknown exception during octopus training.")
          InternalServerError(InternalException("Unknown exception during octopus training."))
        case Failure(err: NotFoundException) =>
          NotFound(NotFoundException(s"Octopus $id does not exist."))
        case Failure(err) =>
          BadRequest(BadRequestException(err.getMessage))
      }
  }

  /**
    * Perform prediction using octopus at id.
    * Currently we support prediction only for datasets with empty SSDs.
    * Note: if a dataset with the provided id does not exist, nothing is done.
    */

  val octopusPredict: Endpoint[SsdResults] = post(APIVersion :: "octopus" :: int :: "predict" :: int) {
    (id: Int, dsID: Int) =>
      Try {
        OctopusInterface.predictOctopus(id, dsID)
      } match {
        case Success(prediction) =>
          Ok(prediction)
        case Failure(err: BadRequestException) =>
          BadRequest(err)
        case Failure(err: InternalException) =>
          InternalServerError(err)
        case Failure(err: NotFoundException) =>
          NotFound(err)
        case Failure(err) =>
          logger.error(s"Octopus $id prediction failed: ${err.getMessage}")
          InternalServerError(InternalException(s"Octopus $id prediction failed"))
      }
  }


  // NOTE: octopus evaluation endpoint is in TestAPI (since we have evaluation independent of a particular octopus)

  /**
    * Patch a portion of a Octopus. Will destroy all cached octopuss
    */
  val octopusPatch: Endpoint[Octopus] = post(APIVersion :: "octopus" :: int :: stringBody) {
    (id: Int, body: String) =>
      (for {
        request <- parseOctopusRequest(body)
        octopus <- Try {
          OctopusInterface.updateOctopus(id, request)

        }
      } yield octopus)
      match {
        case Success(m) =>
          Ok(m)
        case Failure(err: BadRequestException) =>
          BadRequest(err)
        case Failure(err: NotFoundException) =>
          NotFound(err)
        case Failure(err: InternalException) =>
          InternalServerError(err)
        case Failure(err) =>
          logger.error(s"Unknown failure in octopus $id update: ${err.getMessage}")
          InternalServerError(InternalException(s"Failed to update octopus $id."))
      }
  }

  /**
    * Deletes the octopus at position id.
    */
  val octopusDelete: Endpoint[String] = delete(APIVersion :: "octopus" :: int) {
    (id: Int) =>
      Try(OctopusInterface.deleteOctopus(id)) match {
        case Success(Some(_)) =>
          logger.debug(s"Deleted octopus $id")
          Ok(s"Octopus $id deleted successfully.")
        case Success(None) =>
          logger.debug(s"Could not find octopus $id")
          NotFound(NotFoundException(s"Octopus $id could not be found"))
        case Failure(err: BadRequestException) =>
          BadRequest(err)
        case Failure(err: InternalException) =>
          InternalServerError(err)
        case Failure(err) =>
          logger.error(s"Some other problem with deleting octopus $id: ${err.getMessage}")
          InternalServerError(InternalException(s"Failed to delete octopus."))
      }
  }

  /**
    * Return the alignment graph for the octopus at position id.
    */
  val octopusAlignment: Endpoint[String] = get(APIVersion :: "octopus" :: int :: "alignment") {
    (id: Int) =>
      Try { OctopusInterface.getAlignment(id)} match {
        case Success(Some(align: String)) =>
          logger.debug(s"Obtained alignment graph for octopus $id")
          Ok(align)
        case Success(None) =>
          logger.error(s"Could not find octopus $id")
          NotFound(NotFoundException(s"Octopus $id could not be found"))
        case Failure(err) =>
          logger.error(s"Some other problem with obtaining alignment for the octopus $id: ${err.getMessage}")
          InternalServerError(InternalException(s"Failed to get alignment for the octopus."))
      }
  }

  /**
    * Helper function to parse a string into a OctopusRequest object...
    *
    * @param str The json string with the octopus request information
    * @return
    */
  private def parseOctopusRequest(str: String): Try[OctopusRequest] = {

    for {
      raw <- Try { parse(str) }

      description <- HelperJSON.parseOption[String]("description", raw)

      name <- HelperJSON.parseOption[String]("name", raw)

      ssds <- HelperJSON.parseOption[List[SsdID]]("ssds", raw)

      ontologies <- HelperJSON.parseOption[List[OwlID]]("ontologies", raw)

      modelingProperties <- HelperJSON.parseOption[ModelingProperties]("modelingProps", raw)

      modelType <- HelperJSON.parseOption[String]("modelType", raw)
        .map(_.map(
          ModelType.lookup(_)
            .getOrElse(throw BadRequestException("Bad modelType"))))

      features <- HelperJSON.parseOption[FeaturesConfig]("features", raw)

      resamplingStrategy <- HelperJSON.parseOption[String]("resamplingStrategy", raw)
        .map(_.map(
          SamplingStrategy.lookup(_)
            .getOrElse(throw BadRequestException("Bad resamplingStrategy"))))

      bagSize <- HelperJSON.parseOption[Int]("bagSize", raw)

      numBags <- HelperJSON.parseOption[Int]("numBags", raw)

    } yield OctopusRequest(
      name = name,
      description = description,
      modelType = modelType,
      features = features,
      resamplingStrategy = resamplingStrategy,
      numBags = numBags,
      bagSize = bagSize,
      ontologies = ontologies,
      ssds = ssds,
      modelingProps = modelingProperties
    )
  }

  /**
    * Final endpoints for the Octopus endpoint...
    */
  val endpoints =
    octopusRoot :+:
      octopusCreate :+:
      octopusGet :+:
      octopusTrain :+:
      octopusPatch :+:
      octopusDelete :+:
      octopusPredict :+:
      octopusAlignment
}


// needs all the options for the update methods - it
// may be possible to have a separate update request object
case class OctopusRequest(name: Option[String],
                          description: Option[String],
                          modelType: Option[ModelType],
                          features: Option[FeaturesConfig],
                          resamplingStrategy: Option[SamplingStrategy],
                          numBags: Option[Int],
                          bagSize: Option[Int],
                          ontologies: Option[List[Int]],
                          ssds: Option[List[Int]],
                          modelingProps: Option[ModelingProperties])