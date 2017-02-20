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
package au.csiro.data61.core.types

import java.nio.file.Path

import au.csiro.data61.core.types.DataSetTypes.DataSetID
import au.csiro.data61.core.types.MatcherTypes.ModelID


/**
  * Contains the types for the modeler's Alignment
  * calculations
  *
  */
object ModelerTypes {

  type OwlID = Int
  type SsdID = Int
  type AlignmentID = Int

  /**
    * Alignment
    *
    * @param id The ID key for the alignment
    * @param ontologies The ontologies used for the alignment
    * @param ssds The list of SSDs for the data integration alignment
    */
  case class Alignment(id: AlignmentID,
                       modelID: ModelID,
                       ontologies: List[Owl],
                       ssds: List[SsdID]) extends Identifiable[AlignmentID]

  /**
    * Owl is a reference to the Owl file storage
    *
    * @param id The ID key for the OWL file storage
    * @param path The path to the OWL file
    */
  case class Owl(id: OwlID,
                 path: Path) extends Identifiable[OwlID]

  /**
    * SSD is the Semantic Source Description object
    *
    * @param id The ID for the Semantic Source Description
    * @param ontologies The list of ontologies
    * @param dataSet The dataset reference
    */
  case class SSD(id: SsdID,
                 ontologies: List[Owl],
                 dataSet: DataSetID) extends Identifiable[SsdID]

}
