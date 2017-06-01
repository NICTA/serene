package au.csiro.data61.matcher.matcher.train

import au.csiro.data61.matcher.data._

object TrainAliases {
    type DMAttribute = Attribute
    type Scores = Array[Double]
    type Predictions = Seq[(String, Scores)]
    type Features = List[Any]
    type PredictionObject = Seq[(String, Scores, Features)]
}