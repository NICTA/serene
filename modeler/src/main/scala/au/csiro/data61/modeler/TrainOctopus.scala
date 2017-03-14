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

import java.nio.file.{Path, Paths}

import au.csiro.data61.modeler.karma.{KarmaBuildAlignmentGraph, KarmaParams}
import au.csiro.data61.types.Exceptions.ModelerException
import au.csiro.data61.types.SsdTypes.Octopus
import au.csiro.data61.types.Ssd
import com.typesafe.scalalogging.LazyLogging

import scala.util.{Failure, Success, Try}

/**
  * As input we give a list of known ssd, list of ontologies, alignment directory and modeling properties.
  * As output we get the alignment graph.
  */
object TrainOctopus extends LazyLogging{


  def train(octopus: Octopus
            , alignmentDir: Path
            , ontologies: List[String]
            , knownSSDs: List[Ssd]): Option[Path] = {
    Try {
      val karmaWrapper = KarmaParams(alignmentDir = alignmentDir.toString,
        ontologies = ontologies,
        ModelerConfig.makeModelingProps(octopus.modelingProps)
      )

      val alignment = KarmaBuildAlignmentGraph(karmaWrapper).constructInitialAlignment(knownSSDs)

      karmaWrapper.deleteKarma() // deleting karma home directory

      alignmentDir
    }
  } match {
    case Success(path) =>
      Some(path)
    case Failure(err) =>
      logger.error(s"Semantic modeler failed to train the octopus: $err")
      throw ModelerException(s"Semantic modeler failed to train the octopus: $err")
  }
}
