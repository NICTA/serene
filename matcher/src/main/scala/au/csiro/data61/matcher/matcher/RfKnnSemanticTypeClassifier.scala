package au.csiro.data61.matcher.matcher

import au.csiro.data61.matcher.data._
import au.csiro.data61.matcher.matcher.train.TrainAliases._

trait SemanticTypeClassifier {
    def predict(datasets: List[DataModel]): PredictionObject
}

case class RfKnnFeature(val attributeId: String, val attributeName: String, val label: String)
case class RfKnnAuxData(val knnFeatures: List[RfKnnFeature])