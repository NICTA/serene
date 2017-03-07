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
import au.csiro.data61.types.SsdTypes.{Octopus, OctopusID}
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
  *  Octopus REST endpoints...
  *
  *  GET    /v1.0/octopus              -- json list of octopus ids
  *  POST   /v1.0/octopus              -- json octopus object
  *  GET    /v1.0/octopus/:id          -- json octopus object
  *  GET    /v1.0/octopus/:id/train    -- returns async status obj
  *  GET    /v1.0/octopus/:id/predict  -- returns async status obj
  *  POST   /v1.0/octopus/:id          -- update
  *  DELETE /v1.0/octopus/:id
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
    modelingProps = None,
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
    *
    * {
    *   "name": "hello"
    *   "description": "Testing octopus used for identifying phone numbers only.",
    *   "ssds": [1, 2, 3]
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
          request.description match {
            case Some(x) if x.nonEmpty =>
              request
            case _ =>
              throw BadRequestException("No classes found.")
          }
        }
        _ <- Try {
          if (request.name.isEmpty) {
            throw BadRequestException("No features found.")
          }
        }
        m <- Try { OctopusInterface.createOctopus(request) }
      } yield m)
      match {
        case Success(mod) =>
          Ok(mod)
        case Failure(err: InternalException) =>
          InternalServerError(InternalException(err.getMessage))
        case Failure(err) =>
          BadRequest(BadRequestException(err.getMessage))
      }
  }

  /**
    * Returns a JSON O object at id
    */
  val octopusGet: Endpoint[Octopus] = get(APIVersion :: "octopus" :: int) {
    (id: Int) =>
      Try { OctopusInterface.getOctopus(id) } match {
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
      val state = Try(OctopusInterface.trainOctopus(id, force.exists(_.toBoolean)))
      state match {
        case Success(Some(_))  =>
          Accepted[Unit]
        case Success(None) =>
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
          InternalServerError(InternalException(err.getMessage))
      }
  }

  // NOTE: octopus evaluation endpoint --> to be implemented in the python client

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
        case Failure(err) =>
          InternalServerError(InternalException(err.getMessage))
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
        case Failure(err) =>
          logger.debug(s"Some other problem with deleting...")
          InternalServerError(InternalException(s"Failed to delete resource: ${err.getMessage}"))
      }
  }

  /**
    * Helper function to parse json objects. This will return None if
    * nothing is present, and throw a BadRequest error if it is incorrect,
    * and Some(T) if correct
    *
    * @param label The key for the object. Must be present in jValue
    * @param jValue The Json Object
    * @tparam T The return type
    * @return
    */
  private def parseOption[T: Manifest](label: String, jValue: JValue): Try[Option[T]] = {
    val jv = jValue \ label
    if (jv == JNothing) {
      Success(None)
    } else {
      Try {
        Some(jv.extract[T])
      } recoverWith {
        case err =>
          Failure(BadRequestException(s"Failed to parse: $label. Error: ${err.getMessage}"))
      }
    }
  }

  /**
    * Helper function to parse a string into a OctopusRequest object...
    *
    * @param str The json string with the octopus request information
    * @return
    */
  private def parseOctopusRequest(str: String): Try[OctopusRequest] = {

    // TODO: Implement this!
    for {
      raw <- Try {
        parse(str)
      }

      description <- parseOption[String]("description", raw)

      ssds <- parseOption[List[Int]]("ssds", raw)

      name <- parseOption[String]("name", raw)

    } yield OctopusRequest(
      name = Some("junk"),
      description = None,
      modelType = None,
      features = None,
      resamplingStrategy = None,
      numBags = None,
      bagSize = None,
      ontologies = None,
      ssds = None,
      modelingProps = None
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
      octopusPredict
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
                          modelingProps: Option[String])