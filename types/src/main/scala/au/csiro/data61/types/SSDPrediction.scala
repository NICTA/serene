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

package au.csiro.data61.types

import au.csiro.data61.types.SSDTypes.{OctopusID, SsdID}

/**
  * Class which encapsulates all scores calculated for the Semantic Model.
  *
  * @param linkCost       Cost of the links calculated during Steiner Tree Problem
  * @param linkCoherence  Coherence of links
  * @param nodeConfidence Confidence scores for semantic types
  * @param nodeCoherence  Coherence of node mappings
  * @param sizeReduction  Proportion of present nodes/links compared to possible
  * @param nodeCoverage   Proportion of columns mapped to nodes
  */
case class SemanticScores(linkCost: Double,
                          linkCoherence: Double,
                          nodeConfidence: Double,
                          nodeCoherence: Double,
                          sizeReduction: Double,
                          nodeCoverage: Double,
                          karmaScore: Double,
                          karmaRank: Int) {

  def calculateRank: Double = {
    linkCost + linkCoherence + nodeCoherence + nodeConfidence + nodeCoverage
  }

  override def toString: String = {
    s"SemanticScores(\n    linkCost=$linkCost,\n    linkCoherence=$linkCoherence,\n    nodeConfidence=$nodeConfidence," +
      s"\n    nodeCoherence=$nodeCoherence,\n    sizeReduction=$sizeReduction,\n    nodeCoverage=$nodeCoverage," +
      s"\n    karmaRank=$karmaRank)"
  }
}

/**
  * Class which stores the suggested semantic models for a data source.
  * @param ssdID        Id of the original semantic source description
  * @param octopusID    Id of the octopus which was used to generate the suggestions
  * @param suggestions  List of suggested SSDs with associated semantic scores
  */
case class SSDPrediction(ssdID:       SsdID,
                         octopusID:   OctopusID,
                         suggestions: List[(SemanticSourceDesc, SemanticScores)])

/**
  * Return type to user from the Octopus prediction
  * @param predictions Ordered list of predictions
  */
case class SsdResults(predictions: List[(SsdRequest, SemanticScores)])

