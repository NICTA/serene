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


import au.csiro.data61.modeler.{EvaluateOctopus, EvaluationResult}
import au.csiro.data61.types.Exceptions.ModelerException
import au.csiro.data61.types.SsdTypes.OwlID
import au.csiro.data61.types._
import com.typesafe.scalalogging.LazyLogging
import io.finch._
import io.finch.json4s.decodeJson

import scala.util.{Failure, Success, Try}


object TestAPI extends RestAPI {

  val status: Endpoint[StatusMessage] = get(APIVersion) {
    Ok(StatusMessage("ok"))
  }

  val version: Endpoint[VersionMessage] = get(/) {
    Ok(VersionMessage(APIVersion))
  }

  /**
    * Evaluate a predicted SSD against the correct one.
    * No particular octopus is required here.
    * json body of the request:
    * {
        "predictedSsd": {
          "name": "businessInfo.csv",
          "ontologies": [1],
          "semanticModel": {
            "nodes": [***],
            "links": [***]
          },
          "mappings": [***]
        },
        "correctSsd": {
          "name": "businessInfo.csv",
          "ontologies": [1],
          "semanticModel": {
            "nodes": [***],
            "links": [***]
          },
          "mappings": [***]
        },
        "ignoreSemanticTypes": true,
        "ignoreColumnNodes": true
      }
    */
  val octopusEvaluate: Endpoint[EvaluationResult] = post(APIVersion :: "evaluate" :: jsonBody[EvaluationRequest]) {
    (request: EvaluationRequest) =>
      logger.debug("Requesting evaluation for semantic models...")
      request.evaluate() match {
        case Success(res)  =>
          Ok(res)
        case Failure(err: ModelerException) =>
          InternalServerError(InternalException("Evaluation of semantic models failed."))
        case Failure(err: BadRequestException) =>
          BadRequest(err)
        case Failure(err) =>
          logger.error(s"Evaluation of two semantic models failed due to some unforseen reasons: ${err.getMessage}")
          BadRequest(BadRequestException(err.getMessage))
      }
  } handle {
    case e: Exception =>
      logger.error(s"Parsing error ${e.getMessage}")
      BadRequest(BadRequestException(s"Request body cannot be parsed. Check SSD: ${e.getCause}, ${e.getMessage}"))
  }

  val endpoints = version :+: status :+: octopusEvaluate
}

/**
  * EvaluationRequest is the request specification to evaluate
  * a particular predicted semantic model against the correct one.
  *
  * @param predictedSsd Ssd which was predicted by semantic modeler
  * @param correctSsd Correct ssd
  * @param ignoreSemanticTypes Boolean whether correctness of semantic types should be considered or just of the links
  * @param ignoreColumnNodes Boolean to ignore data nodes of the semantic model
  */
case class EvaluationRequest(predictedSsd: SsdRequest,
                             correctSsd: SsdRequest,
                             ignoreSemanticTypes: Boolean = true,
                             ignoreColumnNodes: Boolean = false)
extends LazyLogging {
  val dummyPredSsdId = 1
  val dummyCorrectSsdId = 2

  private def processSsdRequest(ssdReq: SsdRequest): SsdRequest = {
    if (ssdReq.semanticModel.isEmpty) {
      logger.error("SsdRequest has no semantic model. Evaluation makes no sense.")
      throw BadRequestException("SsdRequest has no semantic model.")
    }

    if (ssdReq.mappings.isEmpty) {
      logger.error("SsdRequest has no mappings. Evaluation makes no sense.")
      throw BadRequestException("SsdRequest has no mappings.")
    }

    // ontologies and name can be ignored for the evaluation
    ssdReq.copy(name = Some(ssdReq.name.getOrElse("")),
      ontologies = Some(ssdReq.ontologies.getOrElse(List.empty[Int])))
  }

  def evaluate(): Try[EvaluationResult] = {
    logger.info(s"Launching evaluation for evaluation request ignoring ($ignoreSemanticTypes, $ignoreColumnNodes)")

    for {

      predicted <- processSsdRequest(predictedSsd).toSsd(dummyPredSsdId)
      correct <- processSsdRequest(correctSsd).toSsd(dummyCorrectSsdId)

      eval <- Try {
        EvaluateOctopus.evaluate(predicted,
          correct,
          ignoreSemanticTypes,
          ignoreColumnNodes)
      }
    } yield eval
  }
}

