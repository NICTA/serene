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
import au.csiro.data61.core.types.ModelerTypes.OwlDocumentFormat.OwlDocumentFormat
import au.csiro.data61.core.types.Training.TrainState
import org.joda.time.DateTime


/**
  * Contains the types for the modeler's Alignment
  * calculations
  *
  */
object ModelerTypes {
  type OwlID = Int
  type SsdID = Int
  type OctopusID = Int

  /**
    * Alignment
    *
    * @param id The ID key for the alignment
    * @param ontologies The ontologies used for the alignment
    * @param ssds The list of SSDs for the data integration alignment
    */
  case class Octopus(id: OctopusID,
                     name: String,
                     description: String,
                     modelID: ModelID,
                     ontologies: List[OwlID],
                     ssds: List[SsdID],
                     dateCreated: DateTime,
                     dateModified: DateTime,
                     state: TrainState) extends Identifiable[OctopusID]

  /**
    * Owl is a reference to the Owl file storage.
    *
    * @param id The ID key for the OWL file storage.
    * @param name The name of the original uploaded OWL file.
    * @param format The format of the OWL file.
    * @param description The description of the OWL file.
    * @param dateCreated  The creation time.
    * @param dateModified The last modified time.
    */
  case class Owl(
    id: OwlID,
    name: String,
    format: OwlDocumentFormat,
    description: String,
    dateCreated: DateTime,
    dateModified: DateTime) extends Identifiable[OwlID]

  /**
    * OwlDocumentFormat is the format of the uploaded
    * document. This needs to be specified by the user
    * it is not auto-detected at this stage..
    */
  object OwlDocumentFormat extends Enumeration {
    type OwlDocumentFormat = Value

    val Turtle = Value("turtle")
    val JsonLd = Value("jsonld")
    val RdfXml = Value("rdfxml")
    val Unknown = Value("unknown")
  }

  /**
    * SSD is the Semantic Source Description object
    *
    * @param id The ID for the Semantic Source Description
    * @param ontologies The list of ontologies
    * @param dataSet The dataset reference
    * @param dateCreated The timestamp when the object was created
    * @param dateModified The timestamp when the object is changed
    */
  case class SSD(id: SsdID,
                 ontologies: List[OwlID],
                 dataSet: DataSetID,
                 dateCreated: DateTime,
                 dateModified: DateTime) extends Identifiable[SsdID]

}
