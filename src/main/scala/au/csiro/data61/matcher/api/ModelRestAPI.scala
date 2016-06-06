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

import au.csiro.data61.matcher.types.ModelType.RANDOM_FOREST
import au.csiro.data61.matcher.types.SamplingStrategy._
import au.csiro.data61.matcher._
import io.finch._
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
    modelType = ModelType.RANDOM_FOREST,
    labels = List("name", "address", "phone", "flight"),
    features = List(Feature.IS_ALPHA, Feature.NUM_ALPHA, Feature.NUM_CHARS),
    training = KFold(10),
    costMatrix = List(
      List(1,0,0,0),
      List(0,1,0,0),
      List(0,0,1,0),
      List(0,0,0,1)),
    resamplingStrategy = SamplingStrategy.RESAMPLE_TO_MEAN
  )
  /**
   * Returns all model ids
   *
   * curl http://localhost:8080/v1.0/model
   */
  val modelRoot: Endpoint[List[Int]] = get(APIVersion :: "model") {
    Ok(List(1, 2, 3, 4))
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
            .extract[String]
        }
        modelType <- Try {
          (raw \ "modelType")
            .extractOpt[String]
            .flatMap(ModelType.lookup)
            .getOrElse(RANDOM_FOREST)
        }
        labels <- Try {
          (raw \ "labels")
            .extract[List[String]]
        }
        features <- Try {
          (raw \ "features")
            .extract[List[String]]
            .flatMap(Feature.lookup)
        }
        training <- Try {
          (raw \ "training")
            .extractOpt[KFold]
            .getOrElse(KFold(10))
        }
        costMatrix <- Try {
          (raw \ "costMatrix")
            .extractOpt[List[List[Double]]]
            .getOrElse(List())
        }
        resamplingStrategy <- Try {
          (raw \ "resamplingStrategy")
            .extractOpt[String]
            .flatMap(SamplingStrategy.lookup)
            .getOrElse(RESAMPLE_TO_MEAN)
        }
      } yield Model(
        description,
        modelType,
        labels,
        features,
        training,
        costMatrix,
        resamplingStrategy
      )

      model match {
        case Success(m) =>
          Ok(m)
        case Failure(err) =>
          BadRequest(BadRequestException(err.getMessage))
      }
  }

  /**
   * Returns a JSON Model object at id
   */
  val modelGet: Endpoint[Model] = get(APIVersion :: "model" :: int) {
    (id: Int) =>
      Ok(TestModel)
  }

  /**
   * Patch a portion of a Model. Will destroy all cached models
   */
  val modelPatch: Endpoint[Model] = post(APIVersion :: "model" :: int) {
    (id: Int) =>
      Ok(TestModel)
  }

  /**
   * Replace a Model. Will destroy all cached models
   */
  val modelPut: Endpoint[Model] = put(APIVersion :: "model" :: int) {
    (id: Int) =>
      Ok(TestModel)
  }
  /**
   * Deletes the model at position id.
   */
  val modelDelete: Endpoint[String] = delete(APIVersion :: "model" :: int) {
    (id: Int) =>
      Ok(s"Deleted $id successfully")
  }

  /**
   * Final endpoints for the Dataset endpoint...
   */
  val endpoints =
    modelRoot :+:
      modelCreate :+:
      modelGet :+:
      modelPatch :+:
      modelPut :+:
      modelDelete
}
