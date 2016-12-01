package com.nicta.dataint.matcher.serializable

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.tuning.CrossValidatorModel
import com.nicta.dataint.matcher.features._

import java.io._

case class SerializableMLibClassifier(model: PipelineModel,
                                      classes: List[String],
                                      featureExtractors: List[FeatureExtractor],
                                      postProcessingConfig: Option[Map[String,Any]] = None) extends Serializable