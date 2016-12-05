package com.nicta.dataint.matcher.train

import com.nicta.dataint.matcher.features._

case class TrainingSettings(
    val resamplingStrategy: String,
    val featureSettings: FeatureSettings,
    val costMatrix: Option[Either[String, CostMatrixConfig]] = None,
    val postProcessingConfig: Option[Map[String, Any]] = None)
