package au.csiro.data61.matcher.matcher.train

import au.csiro.data61.matcher.data.LabelTypes._
import au.csiro.data61.matcher.data._
import au.csiro.data61.matcher.matcher._
import au.csiro.data61.matcher.matcher.features._

import au.csiro.data61.matcher.matcher.train.TrainAliases._

import scala.util._

trait TrainSemanticTypeClassifier {
    def train(trainingData: DataModel, 
              labels: SemanticTypeLabels, 
              trainingSettings: TrainingSettings, 
              postProcessingConfig: scala.Option[Map[String,Any]]
             ): SemanticTypeClassifier
}