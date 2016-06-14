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
package au.csiro.data61.matcher.api

import au.csiro.data61.matcher.types.ModelTypes.{Model, ModelID}
import au.csiro.data61.matcher._
import au.csiro.data61.matcher.types.TrainResponses.TrainResponse
import io.finch._
import org.joda.time.DateTime
import org.json4s.jackson.JsonMethods._
import types._

import scala.language.postfixOps
import scala.util.{Failure, Success, Try}



/**
 *  Model REST endpoints...
 *
 *  GET    /v1.0/model
 *  POST   /v1.0/model        -- json object
 *  GET    /v1.0/model/:id
 *  PUT    /v1.0/model/:id    -- json object
 *  PATCH  /v1.0/model/:id    -- json object
 *  DELETE /v1.0/model/:id
 */
object ModelRestAPI extends RestAPI {

  val TestModel = Model(
    description = "This is a model description",
    id = 0,
    modelType = ModelType.RANDOM_FOREST,
    labels = List("name", "address", "phone", "flight"),
    features = List(Feature.IS_ALPHA, Feature.NUM_ALPHA, Feature.NUM_CHARS),
    costMatrix = List(
      List(1,0,0,0),
      List(0,1,0,0),
      List(0,0,1,0),
      List(0,0,0,1)),
    labelData = Map.empty[String, String],
    resamplingStrategy = SamplingStrategy.RESAMPLE_TO_MEAN,
    dateCreated = DateTime.now,
    dateModified = DateTime.now
  )

  /**
   * Returns all model ids
   *
   * curl http://localhost:8080/v1.0/model
   */
  val modelRoot: Endpoint[List[ModelID]] = get(APIVersion :: "model") {
    Ok(MatcherInterface.modelKeys)
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
  val modelCreate: Endpoint[Model] = post(APIVersion :: "model" :: body) {
    (body: String) =>
      val raw = parse(body)

      val model = for {
        description <- Try {
          (raw \ "description")
            .extractOpt[String]
        }
        modelType <- Try {
          (raw \ "modelType")
            .extractOpt[String]
            .flatMap(ModelType.lookup)
        }
        labels <- Try {
          val list = (raw \ "labels").extract[List[String]]
          if (list.isEmpty) {
            throw BadRequestException("No labels found")
          }
          list
        }
        features <- Try {
          (raw \ "features")
            .extractOpt[List[String]]
            .map(_.map(x => Feature.lookup(x).get))
        }
//        training <- Try {
//          (raw \ "training")
//            .extractOpt[KFold]
//        }
        labelData <- Try {
          (raw \ "userData")
            .extractOpt[Map[String, String]]
        }
        costMatrix <- Try {
          (raw \ "costMatrix")
            .extractOpt[List[List[Double]]]
        }
        resamplingStrategy <- Try {
          (raw \ "resamplingStrategy")
            .extractOpt[String]
            .map(SamplingStrategy
              .lookup(_)
              .getOrElse(throw BadRequestException("Bad resamplingStrategy")))
        }
      } yield ModelRequest(
        description,
        modelType,
        labels,
        features,
        costMatrix,
        labelData,
        resamplingStrategy
      )

      (for {
        request <- model
        m <- Try { MatcherInterface.createModel(request) }
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
      Try { MatcherInterface.getModel(id) } match {
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
    */
  val modelTrain: Endpoint[TrainResponse] = get(APIVersion :: "model" :: int :: "train") {
    (id: Int) =>
      val model = Try(MatcherInterface.trainModel(id))
      model match {
        case Success(Some(m))  =>
          Ok(m)
        case Success(None) =>
          NotFound(NotFoundException(s"Model $id does not exist."))
        case Failure(err) =>
          BadRequest(BadRequestException(err.getMessage))
      }
  }

//  /**
//   * Patch a portion of a Model. Will destroy all cached models
//   */
//  val modelPatch: Endpoint[Model] = post(APIVersion :: "model" :: int) {
//    (id: Int) =>
//      Ok(TestModel)
//  }
//
//  /**
//   * Replace a Model. Will destroy all cached models
//   */
//  val modelPut: Endpoint[Model] = put(APIVersion :: "model" :: int) {
//    (id: Int) =>
//      Ok(TestModel)
//  }
//  /**
//   * Deletes the model at position id.
//   */
//  val modelDelete: Endpoint[String] = delete(APIVersion :: "model" :: int) {
//    (id: Int) =>
//      Ok(s"Deleted $id successfully")
//  }

  /**
   * Final endpoints for the Dataset endpoint...
   */
  val endpoints =
    modelRoot :+:
      modelCreate :+:
      modelGet :+:
      modelTrain //:+:
      //modelPatch :+:
      //modelPut :+:
      //modelDelete
}
