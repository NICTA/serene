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

import au.csiro.data61.modeler.karma.{KarmaBuildAlignmentGraph, KarmaParams, KarmaSuggestModel}
import au.csiro.data61.types.ColumnTypes._
import au.csiro.data61.types.Exceptions.ModelerException
import au.csiro.data61.types.SsdTypes._
import au.csiro.data61.types._
import com.typesafe.scalalogging.LazyLogging

import scala.util.Try

/**
  * As input we give a ssd, a list of predicted semantic types for this ssd and directory with alignment graph to be used.
  * As output we get a list of ssd with associated scores.
  */
object PredictOctopus extends LazyLogging {

  /**
    * Generate a ranked list of semantic models based on the provided alignmentGraph and predictions of
    * semantic types.
    * @param octopus
    * @param alignmentDir
    * @param ontologies
    * @param ssd
    * @param dsPredictions
    * @param attrToColMap
    * @param numSemanticTypes
    * @return
    */
  def predict(octopus: Octopus
              , alignmentDir: String
              , ontologies: List[String]
              , ssd: Ssd
              , dsPredictions: Option[DataSetPrediction]
              , attrToColMap: Map[AttrID,ColumnID]
              , numSemanticTypes: Int): Option[SsdPrediction] = {
    logger.info("Semantic Modeler initializes prediction...")

    val karmaWrapper = KarmaParams(alignmentDir = alignmentDir,
      ontologies = ontologies,
      ModelerConfig.makeModelingProps(octopus.modelingProps)
    )

    // TODO: filter unknown class labels!
    val convertedDsPreds: Option[DataSetPrediction] = dsPredictions match {

      case Some(obj: DataSetPrediction) =>
        logger.debug(s"Semantic Modeler got ${obj.predictions.size} dataset predictions.")
        val filteredPreds = filterColumnPredictions(obj.predictions,
          octopus.modelingProps.unknownThreshold)
        logger.info(s"Semantic Modeler will use ${filteredPreds.size} ds predictions.")

        Some(DataSetPrediction(obj.modelID, obj.dataSetID, filteredPreds))

      case None => None
    }

    logger.debug(s"converted ds predictions: ${convertedDsPreds}")

    val suggestions = KarmaSuggestModel(karmaWrapper).suggestModels(ssd
      , ontologies
      , convertedDsPreds
      , octopus.semanticTypeMap
      , attrToColMap
      , numSemanticTypes)

    karmaWrapper.deleteKarma() // deleting karma home directory

    suggestions
  }

  /**
    * Filter problematic column predictions.
    * Those which have all scores 0 and those which are to the unknown class.
    * Identify columns which need to be discarded from the semantic model.
    * Currently they are those which are matched to the unknown class with score > unknownThreshold.
    * Here we generate a new ssd with such columns filtered from attributes.
    * @param columnPreds original column predictions
    * @param unknownThreshold
    * @return
    */
  def filterColumnPredictions(columnPreds: Map[String, ColumnPrediction],
                              unknownThreshold: Double): Map[String, ColumnPrediction] = {

    val filteredPreds: Map[String, ColumnPrediction] =
      columnPreds
        .map {
          case (colId, colPred) =>
            val filterScores = colPred.scores
              .filterKeys(_ != ModelTypes.UknownClass) // we filter unknown class label since it's not in the ontology
            val (maxLab: String, maxScore: Double) =
              if (colPred.scores.getOrElse(ModelTypes.UknownClass, 0.0) > unknownThreshold){
                // discard columns which have match to unknwon class more than threshold
                (ModelTypes.UknownClass, 0.0)
              } else {
                filterScores.maxBy(_._2)
              }
            (colId, colPred.copy(scores = filterScores, confidence = maxScore, label = maxLab))
        }
        .filter(_._2.confidence > 0) // this means that all scores are 0 for this column ~ no predictions

    filteredPreds
  }



  private def tryToInt( s: String ) = Try(s.toInt).toOption
}