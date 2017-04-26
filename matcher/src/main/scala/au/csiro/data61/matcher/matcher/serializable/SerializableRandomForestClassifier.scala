package au.csiro.data61.matcher.matcher.serializable

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.tuning.CrossValidatorModel
import au.csiro.data61.matcher.matcher.features._

import java.io._

@SerialVersionUID(15L)
case class SerializableMLibClassifier(model: PipelineModel,
                                      classes: List[String],
                                      featureExtractors: List[FeatureExtractor],
                                      postProcessingConfig: Option[Map[String,Any]] = None) extends Serializable
