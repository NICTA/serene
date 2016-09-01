/**
  * Copyright (C) 2015-2016 Data61, Commonwealth Scientific and Industrial Research Organisation (CSIRO).
  * See the LICENCE.txt file distributed with this work for additional
  * information regarding copyright ownership.
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package au.csiro.data61.matcher

import java.nio.file.{Files, Paths}

import au.csiro.data61.matcher.api.NotFoundException
import au.csiro.data61.matcher.storage.{DatasetStorage, ModelStorage}
import au.csiro.data61.matcher.types.ModelTypes.{Model, ModelID}
import com.typesafe.scalalogging.LazyLogging

// data integration project
import com.nicta.dataint.data.SemanticTypeLabels
import com.nicta.dataint.matcher.train.{TrainMlibSemanticTypeClassifier, TrainingSettings}
import com.nicta.dataint.matcher.features.FeatureSettings
import com.nicta.dataint.ingestion.loader.CSVHierarchicalDataLoader
import com.nicta.dataint.data.DataModel
import com.nicta.dataint.ingestion.loader.SemanticTypeLabelsLoader
import com.nicta.dataint.matcher.serializable.SerializableMLibClassifier

case class ModelTrainerPaths(curModel: Model,
                             workspacePath: String,
                             featuresConfigPath: String,
                             costMatrixConfigPath: String,
                             labelsDirPath: String)

case class DataintTrainModel(classes: List[String],
                             trainingSet: DataModel,
                             labels: SemanticTypeLabels,
                             trainSettings: TrainingSettings,
                             postProcessingConfig: Option[Map[String,Any]])

object ModelTrainer extends LazyLogging {

  val rootDir: String = ModelStorage.rootDir
  val datasetDir: String = DatasetStorage.rootDir

  /**
    * Return an instance of class TrainingSettings
    */
  def readSettings(trainerPaths: ModelTrainerPaths): TrainingSettings = {

    val featuresConfig = FeatureSettings.load(
      trainerPaths.featuresConfigPath,
      trainerPaths.workspacePath)

    TrainingSettings(
      trainerPaths.curModel.resamplingStrategy.str,
      featuresConfig,
      Some(Left(trainerPaths.costMatrixConfigPath)))
  }

  /**
    * Returns a list of DataModel instances at path
    */
  def getDataModels(path: String): List[DataModel] = {

    logger.debug(s"getDataModels called for path: $path")

    DatasetStorage
      .getCSVResources
      .map{CSVHierarchicalDataLoader().readDataSet(_, "")}
  }

  /**
    * Returns a list of DataModel instances for the dataset repository
    */
  def readTrainingData: DataModel = {

    logger.debug(s"Reading training data")

    val datasets = getDataModels(datasetDir)

    if (datasets.isEmpty) { // training dataset has to be non-empty
      logger.error("No csv training datasets have been found.")
      throw NotFoundException("No csv training datasets have been found.")
    }

    new DataModel("", None, None, Some(datasets))
  }

  /**
    * Reads in labeled data
    */
  def readLabeledData(trainerPaths: ModelTrainerPaths): SemanticTypeLabels ={

    logger.debug(s"Reading label data from $trainerPaths")

    val labelsLoader = SemanticTypeLabelsLoader()
    val stl = labelsLoader.load(trainerPaths.labelsDirPath)

    if (stl.labelsMap.isEmpty) {// we do not allow unsupervised setting; labeled data should not be empty
      logger.error("No labeled datasets have been found.")
      throw NotFoundException("No labeled datasets have been found.")
    }
    stl
  }

  /**
    * Performs training for the model and returns serialized object for the learnt model
    */
  def train(id: ModelID): Option[SerializableMLibClassifier] = {

    logger.debug(s"train called for model $id")

    ModelStorage.identifyPaths(id)
      .map(cts  => {

        if (!Files.exists(Paths.get(cts.workspacePath))) {
          val msg = s"Workspace directory for the model $id does not exist."
          logger.error(msg)
          throw NotFoundException(msg)
        }

        logger.debug("Attempting to create training object..")

        val dataTrainModel = DataintTrainModel(
          classes = cts.curModel.classes,
          trainingSet = readTrainingData,
          labels = readLabeledData(cts),
          trainSettings = readSettings(cts),
          postProcessingConfig = None)

        logger.debug(s"Created data training model: $dataTrainModel")

        dataTrainModel
      })
      .map(dt => {

        val trainer = TrainMlibSemanticTypeClassifier(dt.classes, doCrossValidation = false)

        val randomForestSchemaMatcher = trainer.train(
          dt.trainingSet,
          dt.labels,
          dt.trainSettings,
          dt.postProcessingConfig)

        SerializableMLibClassifier(
          randomForestSchemaMatcher.model,
          dt.classes,
          randomForestSchemaMatcher.featureExtractors,
          randomForestSchemaMatcher.postProcessingConfig)
      })

  }

}
