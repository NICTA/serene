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
package au.csiro.data61.matcher.matcher.train

import au.csiro.data61.matcher.matcher.features._

case class TrainingSettings(
    resamplingStrategy: String,
    featureSettings: FeatureSettings,
    costMatrix: Option[Either[String, CostMatrixConfig]] = None,
    postProcessingConfig: Option[Map[String, Any]] = None,
    numBags: Option[Int] = None,
    bagSize: Option[Int] = None)

object DefaultBagging {
  val NumBags = 50
  val BagSize = 100
}

case class BaggingParams(numBags: Int = DefaultBagging.NumBags,
                         bagSize: Int = DefaultBagging.BagSize
                        )