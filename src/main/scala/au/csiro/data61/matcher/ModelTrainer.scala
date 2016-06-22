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

import java.io.{FileInputStream, IOException, ObjectInputStream}
import java.nio.file.Paths

import au.csiro.data61.matcher.api.{BadRequestException, NotFoundException}
import au.csiro.data61.matcher.storage.{DatasetStorage, ModelStorage}
import au.csiro.data61.matcher.types.ModelTypes.{Model, ModelID}
import org.joda.time.DateTime
import com.typesafe.scalalogging.LazyLogging

import scala.util.{Failure, Success, Try}

// data integration project
import com.nicta.dataint.data.{DataModel, SemanticTypeLabels}
import com.nicta.dataint.matcher.train.{TrainMlibSemanticTypeClassifier, TrainingSettings}
import com.nicta.dataint.matcher.features.FeatureSettings
import com.nicta.dataint.ingestion.loader.{CSVHierarchicalDataLoader, SemanticTypeLabelsLoader}
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

  /*
   Return an instance of class TrainingSettings
    */
  def readSettings(trainerPaths: ModelTrainerPaths): TrainingSettings = {
    val featuresConfig = FeatureSettings.load(trainerPaths.featuresConfigPath, trainerPaths.workspacePath)
    TrainingSettings(trainerPaths.curModel.resamplingStrategy.str,
      featuresConfig,
      Some(Left(trainerPaths.costMatrixConfigPath)))
  }

  /*
   Returns a list of DataModel instances at path
    */
  def getDataModels(path: String): List[DataModel] = {
    DatasetStorage
      .getCSVResources
      .map{CSVHierarchicalDataLoader().readDataSet(_,"")}
  }

  /*
   Returns a list of DataModel instances for the dataset repository
    */
  def readTrainingData: DataModel = {
    val datasets = getDataModels(datasetDir)
    if(datasets.length < 1){// training dataset has to be non-empty
      logger.error("No csv training datasets have been found.")
      throw NotFoundException("No csv training datasets have been found.")
    }
    new DataModel("", None, None, Some(datasets))
  }

  /*
   Reads in labeled data
    */
  def readLabeledData(trainerPaths: ModelTrainerPaths): SemanticTypeLabels ={
    val labelsLoader = SemanticTypeLabelsLoader()
    val stl = labelsLoader.load(trainerPaths.labelsDirPath)
    if(stl.labelsMap.size < 1){// we do not allow unsupervised setting; labeled data should not be empty
      logger.error("No labeled datasets have been found.")
      throw NotFoundException("No labeled datasets have been found.")
    }
    stl
  }

  /**
    * Performs training for the model and returns serialized object for the learnt model
    *
    * @param id id of the model
    * @return Serialized Mlib classifier wrapped in Option
    */
  def train(id: ModelID): Option[SerializableMLibClassifier] = {
      ModelStorage.identifyPaths(id)
        .map(cts => {
          if (!Paths.get(cts.workspacePath, "").toFile.exists) {
            logger.error(s"Workspace directory for the model $id does not exist.")
            throw NotFoundException(s"Workspace directory for the model $id does not exist.")
          }
          DataintTrainModel(classes = cts.curModel.classes,
            trainingSet = readTrainingData,
            labels = readLabeledData(cts),
            trainSettings = readSettings(cts),
            postProcessingConfig = None)
        })
        .map(dt => {
          val trainer = TrainMlibSemanticTypeClassifier(dt.classes, false)

          val randomForestSchemaMatcher = trainer.train(dt.trainingSet,
            dt.labels,
            dt.trainSettings,
            dt.postProcessingConfig)

          SerializableMLibClassifier(randomForestSchemaMatcher.model
            , dt.classes
            , randomForestSchemaMatcher.featureExtractors
            //,randomForestSchemaMatcher.postProcessingConfig
          )
        })
    }

}
