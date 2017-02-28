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
import au.csiro.data61.types.SSDTypes._
import au.csiro.data61.types.{DataSetPrediction, SSDPrediction, SemanticSourceDesc}
import com.typesafe.scalalogging.LazyLogging

/**
  * As input we give a ssd, a list of predicted semantic types for this ssd and directory with alignment graph to be used.
  * As output we get a list of ssd with associated scores.
  */
object PredictOctopus extends LazyLogging {
  // TODO: to be implemented once AlignmentStorage layer is up
  // delete karma-dir??

  def predict(octopus: Octopus
              , ontologies: List[String]
              , ssd: SemanticSourceDesc
              , dsPredictions: Option[DataSetPrediction]
              , attrToColMap: Map[AttrID,ColumnID]
              , numSemanticTypes: Int): Option[SSDPrediction] = {
    logger.info("Semantic Modeler initializes prediction...")

    val karmaWrapper = KarmaParams(alignmentDir = octopus.alignmentDir
      .getOrElse(throw ModelerException("Alignment directory is not specified in octopus!")).toString,
      ontologies = ontologies,
      None)

    val suggestions = KarmaSuggestModel(karmaWrapper).suggestModels(ssd
      , octopus.id
      , ontologies
      , dsPredictions
      , octopus.semanticTypeMap.getOrElse(Map.empty[String, String])
      , attrToColMap: Map[AttrID,ColumnID]
      , numSemanticTypes)

    karmaWrapper.deleteKarma() // deleting karma home directory

    suggestions
  }
}
