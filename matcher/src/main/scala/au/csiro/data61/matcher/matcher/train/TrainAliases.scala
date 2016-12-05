package com.nicta.dataint.matcher.train

import com.nicta.dataint.data._

object TrainAliases {
    type DMAttribute = Attribute
    type Scores = Array[Double]
    type Predictions = Seq[(Attribute, Scores)]
    type Features = List[Any]
    type PredictionObject = Seq[(Attribute, Scores, Features)]
}