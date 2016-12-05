package au.csiro.data61.matcher.matcher.train

import au.csiro.data61.matcher.matcher.features._

case class TrainingSettings(
    val resamplingStrategy: String,
    val featureSettings: FeatureSettings,
    val costMatrix: Option[Either[String, CostMatrixConfig]] = None,
    val postProcessingConfig: Option[Map[String, Any]] = None)
