package com.nicta.dataint.matcher

import com.nicta.dataint.data._
import com.nicta.dataint.matcher.train.TrainAliases._

trait SemanticTypeClassifier {
    def predict(datasets: List[DataModel]): PredictionObject
}

case class RfKnnFeature(val attributeId: String, val attributeName: String, val label: String)
case class RfKnnAuxData(val knnFeatures: List[RfKnnFeature])