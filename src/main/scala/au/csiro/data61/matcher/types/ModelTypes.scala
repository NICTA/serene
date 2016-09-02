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

import au.csiro.data61.matcher.types.ColumnTypes.ColumnID
import au.csiro.data61.matcher.types.DataSetTypes.DataSetID
import au.csiro.data61.matcher.types.ModelTypes.ModelID
import com.nicta.dataint.matcher.train.TrainAliases.PredictionObject
import org.joda.time.DateTime
import org.json4s._

import scala.language.postfixOps

object ModelTypes {

  case class Model(description: String,
                   id: ModelID,
                   modelType: ModelType,
                   classes: List[String],
                   features: FeaturesConfig,
                   costMatrix: List[List[Double]],
                   resamplingStrategy: SamplingStrategy,
                   labelData: Map[Int, String], // WARNING: Int should be ColumnID! Json4s bug.
                   refDataSets: List[Int],
                   state: TrainState,
                   dateCreated: DateTime,
                   dateModified: DateTime) extends Identifiable[ModelID]

  type ModelID = Int

  /**
   * Enumerated type for the status of training for the model
   */
  sealed trait Status { def str: String }
  object Status {
    case object ERROR extends Status { val str = "error" }
    case object UNTRAINED extends Status { val str = "untrained" }
    case object BUSY extends Status { val str = "busy" }
    case object COMPLETE extends Status { val str = "complete" }

    val values = Set(
      ERROR,
      UNTRAINED,
      BUSY,
      COMPLETE
    )

    def lookup(str: String): Option[Status] = {
      values.find(_.str == str)
    }
  }

  /**
   * Serializer for the State of the trainer
   */
  case object StatusSerializer extends CustomSerializer[Status](format => (
    {
      case jv: JValue =>
        implicit val formats = DefaultFormats
        val str = jv.extract[String]
        val state = Status.lookup(str)
        state getOrElse (throw new Exception("Failed to parse State"))
    }, {
    case state: Status =>
      JString(state.str)
  }))

  /**
   * Training state
   * @param status The current state of the model training
   * @param dateCreated The time it was first created
   * @param dateModified The last time the state changed
   */
  case class TrainState(status: Status,
                        message: String,
                        dateCreated: DateTime,
                        dateModified: DateTime)
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
  * Special type for FeaturesConfig
  */
case class FeaturesConfig(activeFeatures: Set[String],
                            activeGroupFeatures: Set[String],
                            featureExtractorParams: Map[String, Map[String,String]])

// TODO: type-map is part of  featureExtractorParams, type-maps need to be read from datasetrepository when model gets created


case object FeaturesConfigSerializer extends CustomSerializer[FeaturesConfig](format => (
  {
    case jv: JValue =>
      implicit val formats = DefaultFormats
      // TODO: check input feature names, raise warnings if some are incorrectly provided
      val activeFeatures = (jv \ "activeFeatures").extract[List[String]].toSet // TODO: convert to List[Feature]
      val activeGroupFeatures = (jv \ "activeFeatureGroups").extract[List[String]].toSet
      val featureExtractorParams = (jv \ "featureExtractorParams")
        .extract[List[Map[String,String]]]
        .map { case feParams =>
          (feParams("name"), feParams)
        } toMap

  FeaturesConfig(activeFeatures, activeGroupFeatures, featureExtractorParams)
}, {
  case feature: FeaturesConfig =>
    implicit val formats = DefaultFormats
    JObject(List(
      "activeFeatures" -> JArray(feature.activeFeatures.toList.map(JString)),
      "activeFeatureGroups" -> JArray(feature.activeGroupFeatures.toList.map(JString)),
      "featureExtractorParams" -> Extraction.decompose(feature.featureExtractorParams.values)
    ))
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

/**
  * Column prediction
  */
case class ColumnPrediction(id: ModelID,
                            datasetID: DataSetID,
                            columnID: ColumnID,
                            label: String,
                            confidence: Double,
                            scores: Map[String,Double],
                            features: Map[String, Double]
                           ) extends Identifiable[ModelID]