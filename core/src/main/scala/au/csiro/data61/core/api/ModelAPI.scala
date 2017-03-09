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

import au.csiro.data61.core.drivers.ModelInterface
import au.csiro.data61.types.ModelTypes.{Model, ModelID}
import au.csiro.data61.types._
import au.csiro.data61.types.ModelType
import io.finch._
import org.joda.time.DateTime
import org.json4s.JValue
import org.json4s.JsonAST.JNothing
import org.json4s.jackson.JsonMethods._

import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

/**
 *  Model REST endpoints...
 *
 *  GET    /v1.0/model              -- json list of model ids
 *  POST   /v1.0/model              -- json model object
 *  GET    /v1.0/model/:id          -- json model object
 *  GET    /v1.0/model/:id/train    -- returns async status obj
 *  GET    /v1.0/model/:id/predict  -- returns async status obj
 *  POST   /v1.0/model/:id          -- update
 *  DELETE /v1.0/model/:id
 */
object ModelAPI extends RestAPI {

  val TestModel = Model(
    description = "This is a model description",
    id = 0,
    modelType = ModelType.RANDOM_FOREST,
    classes = List("name", "address", "phone", "flight"),
    features = FeaturesConfig(
      activeFeatures = Set(
        "num-unique-vals",
        "prop-unique-vals",
        "prop-missing-vals"
      ),
      activeGroupFeatures = Set(
        "stats-of-text-length",
        "prop-instances-per-class-in-knearestneighbours"
      ),
      featureExtractorParams = Map(
        "prop-instances-per-class-in-knearestneighbours" -> Map(
          "name" -> "prop-instances-per-class-in-knearestneighbours",
          "num-neighbours" -> "5")
        )),
    costMatrix = List(
      List(1,0,0,0),
      List(0,1,0,0),
      List(0,0,1,0),
      List(0,0,0,1)),
    labelData = Map.empty[Int, String],
    resamplingStrategy = SamplingStrategy.RESAMPLE_TO_MEAN,
    modelPath = None,
    refDataSets = List(1, 2, 3, 4),
    state = Training.TrainState(Training.Status.UNTRAINED, "", DateTime.now),
    dateCreated = DateTime.now,
    dateModified = DateTime.now
  )

  /**
   * Returns all model ids
   *
   * curl http://localhost:8080/v1.0/model
   */
  val modelRoot: Endpoint[List[ModelID]] = get(APIVersion :: "model") {
    Ok(ModelInterface.storageKeys)
  }

  /**
   * Adds a new model as specified by the json body.
   *
   * {
   *   "description": "Testing model used for identifying phone numbers only."
   *   "modelType": "randomForest",
   *   "labels": ["name", "address", "phone", "unknown"],
   *   "features": ["isAlpha", "alphaRatio", "atSigns", ...]
   *   "training": {"type": "kFold", "n": 10},
   *   "costMatrix": [[1, 2, 3], [3, 4, 5], [6, 7, 8]],
   *   "resamplingStrategy": "resampleToMean",
   * }
   *
   * Returns a JSON Model object with id.
   *
   */
  val modelCreate: Endpoint[Model] = post(APIVersion :: "model" :: stringBody) {
    (body: String) =>
      (for {
        request <- parseModelRequest(body)
        _ <- Try {
          request.classes match {
            case Some(x) if x.nonEmpty =>
              request
            case _ =>
              throw BadRequestException("No classes found.")
          }
        }
        _ <- Try {
          if (request.features.isEmpty) {
            throw BadRequestException("No features found.")
          }
        }
        m <- Try { ModelInterface.createModel(request) }
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
   * Returns a JSON Model object at id
   */
  val modelGet: Endpoint[Model] = get(APIVersion :: "model" :: int) {
    (id: Int) =>
      Try { ModelInterface.get(id) } match {
        case Success(Some(ds))  =>
          Ok(ds)
        case Success(None) =>
          NotFound(NotFoundException(s"Model $id does not exist."))
        case Failure(err) =>
          BadRequest(BadRequestException(err.getMessage))
      }
  }

  /**
    * Trains a model at id
    * If training has been successfully launched, it returns nothing
    */
  val modelTrain: Endpoint[Unit] = post(APIVersion :: "model" :: int :: "train" :: paramOption("force")) {
    (id: Int, force: Option[String]) =>
      val state = Try(ModelInterface.trainModel(id, force.exists(_.toBoolean)))
      state match {
        case Success(Some(_))  =>
          Accepted[Unit]
        case Success(None) =>
          NotFound(NotFoundException(s"Model $id does not exist."))
        case Failure(err) =>
          BadRequest(BadRequestException(err.getMessage))
      }
  }

  /**
    * Perform prediction using model at id.
    * Prediction is performed asynchronously.
    * If no datasetID parameter is provided, prediction is performed for all datasets in the repo.
    * If a datasetID parameter is provided, prediction is performed only for the dataset with such datasetID.
    * Note: if a dataset with the provided id does not exist, nothing is done.
    */
  // auxiliary endpoint for the optional datasetID parameter
  //val dsParam: Endpoint[Option[Int]] = paramOption("datasetID").as[Int]

  val modelPredict: Endpoint[DataSetPrediction] = post(APIVersion :: "model" :: int :: "predict" :: int) {
    (id: Int, datasetID: Int) =>
      Try {
        ModelInterface.predictModel(id, datasetID)
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
          logger.error(s"Some other error occurred in model $id prediction: ${err.getMessage}")
          InternalServerError(InternalException("Model prediction failed."))
      }
  }

  // NOTE: model evaluation endpoint --> to be implemented in the python client

  /**
   * Patch a portion of a Model. Will destroy all cached models
   */
  val modelPatch: Endpoint[Model] = post(APIVersion :: "model" :: int :: stringBody) {
    (id: Int, body: String) =>
      (for {
        request <- parseModelRequest(body)
        model <- Try {
          ModelInterface.updateModel(id, request)
        }
      } yield model)
      match {
        case Success(m) =>
          Ok(m)
        case Failure(err: NotFoundException) =>
          NotFound(err)
        case Failure(err: BadRequestException) =>
          BadRequest(err)
        case Failure(err: InternalException) =>
          InternalServerError(err)
        case Failure(err) =>
          logger.error(s"Some other problem with updating model $id: ${err.getMessage}")
          InternalServerError(InternalException(s"Failed to update model $id."))
      }
  }

  /**
   * Deletes the model at position id.
   */
  val modelDelete: Endpoint[String] = delete(APIVersion :: "model" :: int) {
    (id: Int) =>
      Try(ModelInterface.delete(id)) match {
        case Success(Some(_)) =>
          logger.debug(s"Deleted model $id")
          Ok(s"Model $id deleted successfully.")
        case Success(None) =>
          logger.debug(s"Could not find model $id")
          NotFound(NotFoundException(s"Model $id could not be found"))
        case Failure(err: BadRequestException) =>
          BadRequest(err)
        case Failure(err: InternalException) =>
          InternalServerError(err)
        case Failure(err) =>
          logger.error(s"Some other problem with model $id deletion: ${err.getMessage}")
          InternalServerError(InternalException(s"Failed to delete resource."))
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
          Failure(
            BadRequestException(s"Failed to parse: $label. Error: ${err.getMessage}"))
      }
    }
  }

  /**
   * Helper function to parse a string into a ModelRequest object...
   *
   * @param str The json string with the model request information
   * @return
   */
  private def parseModelRequest(str: String): Try[ModelRequest] = {

    for {
      raw <- Try { parse(str) }

      description <- parseOption[String]("description", raw)

      modelType <- parseOption[String]("modelType", raw)
                    .map(_.map(
                      ModelType.lookup(_)
                        .getOrElse(throw BadRequestException("Bad modelType"))))

      classes <- parseOption[List[String]]("classes", raw)

      features <- parseOption[FeaturesConfig]("features", raw)

      userData <- parseOption[Map[Int, String]]("labelData", raw)

      costMatrix <- parseOption[List[List[Double]]]("costMatrix", raw)

      resamplingStrategy <- parseOption[String]("resamplingStrategy", raw)
                              .map(_.map(
                                SamplingStrategy.lookup(_)
                                  .getOrElse(throw BadRequestException("Bad resamplingStrategy"))))

      bagSize <- parseOption[Int]("bagSize", raw)

      numBags <- parseOption[Int]("numBags", raw)
    } yield {
      ModelRequest(
        description,
        modelType,
        classes,
        features,
        costMatrix,
        userData,
        resamplingStrategy,
        numBags,
        bagSize
      )}
  }

  /**
   * Final endpoints for the Model endpoint...
   */
  val endpoints =
    modelRoot :+:
      modelCreate :+:
      modelGet :+:
      modelTrain :+:
      modelPatch :+:
      modelDelete :+:
      modelPredict
}


case class ModelRequest(description: Option[String],
                        modelType: Option[ModelType],
                        classes: Option[List[String]],
                        features: Option[FeaturesConfig],
                        costMatrix: Option[List[List[Double]]],
                        labelData: Option[Map[Int, String]],
                        resamplingStrategy: Option[SamplingStrategy],
                        numBags: Option[Int],
                        bagSize: Option[Int])
