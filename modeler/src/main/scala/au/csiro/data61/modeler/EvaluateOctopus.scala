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
package au.csiro.data61.modeler

import au.csiro.data61.modeler.karma.KarmaParams
import au.csiro.data61.types.Exceptions.ModelerException
import au.csiro.data61.types.{KarmaSemanticModel, Ssd}
import com.typesafe.scalalogging.LazyLogging

import scala.util.{Failure, Success, Try}

/**
  * This object is not actually bound to a particular octopus.
  * It compares two SSDs and returns the results.
  */
object EvaluateOctopus extends LazyLogging {

  private def convertSsdToKarma(ssd: Ssd,
                                karmaWrapper: KarmaParams): KarmaSemanticModel = {
    Try { ssd.toKarmaSemanticModel(karmaWrapper.karmaWorkspace.getOntologyManager)} match {
      case Success(Some(karmaSM)) =>
        karmaSM
      case Success(None) =>
        logger.error("Evaluation is not possible since conversion of predicted SSD to karma representation failed:" +
          "either mapping or semantic model are missing.")
        throw ModelerException("Evaluation is not possible. Conversion failed.")
      case Failure(err) =>
        logger.error("Evaluation is not possible since conversion of predicted SSD to karma representation failed:" +
          s"${err.getMessage}.")
        throw ModelerException("Evaluation is not possible. Conversion failed.")
    }
  }

  /**
    * Compare two SSDs using Karma evaluation metrics.
    * First, each SSD will be converted to a set of RDF triplets.
    * Then two sets of triplets will be compared and corresponding metrics will be calculated.
    * The metrics include precision, recall and jaccard.
    * In all three the numerator is the norm of the intersection of two sets of triplets.
    * The denominator is different: precision - the norm of the predicted set of triplets,
    * recall - the norm of the correct set of triplets,
    * jaccard - the norm of the union of two sets.
    * Graph edit distance will not be computed.
    *
    * @param predictedSsd
    * @param correctSsd
    * @param ignoreSemanticTypes
    * @param ignoreColumnNodes
    * @return
    */
  def evaluate(predictedSsd: Ssd,
               correctSsd: Ssd,
               ignoreSemanticTypes: Boolean = false,
               ignoreColumnNodes: Boolean = false): EvaluationResult = {
    logger.info("Semantic Modeler initializes Karma to perform evaluation of two SSDs...")

    // for karma initialization we can use everywhere the default values since it does not matter here
    val karmaWrapper = KarmaParams(alignmentDir = ModelerConfig.DefaultAlignmenDir,
      ontologies = List(),
      None)

    logger.debug("Converting our SSDs to Karma representation...")
    // convert predicted Ssd to Karma representation
    val karmaPredictedSM = convertSsdToKarma(predictedSsd, karmaWrapper)
    // convert correct Ssd to Karma representation
    val karmaBaseSM = convertSsdToKarma(correctSsd, karmaWrapper)

    logger.debug("Calling Karma Evaluation function..")
    // this Karma method will not calculate distance
    val karmaRes = Try { karmaPredictedSM.karmaModel.evaluate(karmaBaseSM.karmaModel,
      ignoreSemanticTypes, ignoreColumnNodes) } match {
      case Success(res) =>
        res
      case Failure(err) =>
        logger.warn(s"Karma failed to evaluate the two SSDs: ${err.getMessage}")
        throw ModelerException("Evaluation is not possible. Karma failed.")
    }

    // Karma rounds the values to two decimals
    EvaluationResult(karmaRes.getPrecision,
      karmaRes.getRecall,
      karmaRes.getJaccard)
  }

}

/**
  * A wrapper for results which Karma will return for comparison of two semantic models.
  *
  * @param precision 0 is the default value
  * @param recall 0 is the default value
  * @param jaccard 0 is the default value
  */
case class EvaluationResult(precision: Double,
                            recall: Double,
                            jaccard: Double)