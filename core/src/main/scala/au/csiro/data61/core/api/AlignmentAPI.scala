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
import au.csiro.data61.types.{FeaturesConfig, ModelType, SamplingStrategy, StatusMessage}
import io.finch._

import scala.language.postfixOps

/**
 * Alignment application object. Here we compose the endpoints
 * and serve as a Finagle Http Service forever.
 *
 */
object AlignmentAPI extends RestAPI {

  val alignmentRoot: Endpoint[StatusMessage] = get(APIVersion :: "alignment") {
    Ok(StatusMessage("Not Implemented"))
  }

  val alignmentGet: Endpoint[StatusMessage] = get(APIVersion :: "alignment" :: int) {
    (id: Int) =>
      Ok(StatusMessage("Not Implemented"))
  }

  val endpoints = alignmentRoot :+: alignmentGet
}

case class OctopusRequest(description: Option[String],
                          modelType: Option[ModelType],
                          features: Option[FeaturesConfig],
                          resamplingStrategy: Option[SamplingStrategy],
                          numBags: Option[Int],
                          bagSize: Option[Int],
                          ontologies: Option[List[Int]],
                          ssds: Option[List[Int]],
                          modelingProps: Option[String])

//id: OctopusID,
//ontologies: List[Int], // WARNING: Int should be OwlID! Json4s bug.
//ssds: List[Int],       // WARNING: Int should be SsdID! Json4s bug.
//features: FeaturesConfig,
//resamplingStrategy: SamplingStrategy,
//numBags: Option[Int],
//bagSize: Option[Int],
//lobsterID: ModelID,
//modelingProps: Option[String],
//alignmentDir: Option[Path],
//state: TrainState,
//dateCreated: DateTime,
//dateModified: DateTime,
//description: String