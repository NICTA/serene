package com.nicta.dataint.matcher.train

import com.nicta.dataint.data.LabelTypes._
import com.nicta.dataint.data._
import com.nicta.dataint.matcher._
import com.nicta.dataint.matcher.features._

import com.nicta.dataint.matcher.train.TrainAliases._

import scala.util._

trait TrainSemanticTypeClassifier {
    def train(trainingData: DataModel, 
              labels: SemanticTypeLabels, 
              trainingSettings: TrainingSettings, 
              postProcessingConfig: scala.Option[Map[String,Any]]
             ): SemanticTypeClassifier
}