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
package au.csiro.data61.matcher.types

import org.joda.time.DateTime
import org.json4s._

object ModelTypes {

  case class Model(description: String,
                   id: ModelID,
                   modelType: ModelType,
                   labels: List[String],
                   features: List[Feature],
                   training: KFold,
                   costMatrix: List[List[Double]],
                   resamplingStrategy: SamplingStrategy,
                   dateCreated: DateTime,
                   dateModified: DateTime)

  type ModelID = Int
}


/**
 * Enumerated type for the Model type used
 */
sealed trait ModelType { def str: String }

object ModelType {

  case object RANDOM_FOREST extends ModelType { val str = "randomForest" }

  val values = List(
    RANDOM_FOREST
  )

  def lookup(str: String): Option[ModelType] = {
    values.find(_.str == str)
  }
}

case object ModelTypeSerializer extends CustomSerializer[ModelType](format => (
  {
    case jv: JValue =>
      implicit val formats = DefaultFormats
      val str = jv.extract[String]
      val mt = ModelType.lookup(str)
      mt getOrElse (throw new Exception("Failed to parse ModelType"))
  }, {
  case mt: ModelType =>
    JString(mt.str)
}))


/**
 * Enumerated type for the list of features
 */
sealed trait Feature { def str: String }

object Feature {
  case object IS_ALPHA extends Feature { val str = "isAlpha" }
  case object NUM_CHARS extends Feature { val str = "numChars" }
  case object NUM_ALPHA extends Feature { val str = "numAlpha" }

  val values = Set(
    IS_ALPHA,
    NUM_CHARS,
    NUM_ALPHA
  )

  def lookup(str: String): Option[Feature] = {
    values.find(_.str == str)
  }
}

case object FeatureSerializer extends CustomSerializer[Feature](format => (
  {
    case jv: JValue =>
      implicit val formats = DefaultFormats
      val str = jv.extract[String]
      val feature = Feature.lookup(str)
      feature getOrElse (throw new Exception("Failed to parse Feature"))
  }, {
  case feature: Feature =>
    JString(feature.str)
}))


/**
 * Enumerated type for the sampling strategy
 */
sealed trait SamplingStrategy { def str: String }

object SamplingStrategy {
  case object UPSAMPLE_TO_MAX  extends SamplingStrategy { val str = "UpsampleToMax" }
  case object RESAMPLE_TO_MEAN extends SamplingStrategy { val str = "ResampleToMean" }
  case object UPSAMPLE_TO_MEAN extends SamplingStrategy { val str = "UpsampleToMean" }

  val values = Set(
    UPSAMPLE_TO_MAX,
    RESAMPLE_TO_MEAN,
    UPSAMPLE_TO_MEAN
  )

  def lookup(str: String): Option[SamplingStrategy] = {
    values.find(_.str == str)
  }
}

case object SamplingStrategySerializer extends CustomSerializer[SamplingStrategy](format => (
  {
    case jv: JValue =>
      implicit val formats = DefaultFormats
      val str = jv.extract[String]
      val samplingStrategy = SamplingStrategy.lookup(str)
      samplingStrategy getOrElse (throw new Exception("Failed to parse SamplingStrategy"))
  }, {
  case samplingStrategy: SamplingStrategy =>
    JString(samplingStrategy.str)
}))



/**
 * KFold
 *
 * @param n
 */
case class KFold(n: Int)

