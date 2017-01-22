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
package au.csiro.data61.core.drivers

import java.io._
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import au.csiro.data61.core.api.{InternalException, NotFoundException}
import au.csiro.data61.core.storage.ModelStorage._
import au.csiro.data61.core.storage.{DatasetStorage, ModelStorage}
import au.csiro.data61.core.types.{MatcherJsonFormats, ModelFeatureExtractors}
import au.csiro.data61.core.types.ModelTypes.{Model, ModelID}
import au.csiro.data61.matcher.matcher.features._
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.language.postfixOps
import scala.util.{Failure, Success, Try}


// data integration project
import au.csiro.data61.matcher.data.{DataModel, SemanticTypeLabels}
import au.csiro.data61.matcher.ingestion.loader.{CSVDataLoader, SemanticTypeLabelsLoader}
import au.csiro.data61.matcher.matcher.features.FeatureSettings
import au.csiro.data61.matcher.matcher.serializable.SerializableMLibClassifier
import au.csiro.data61.matcher.matcher.train.{TrainMlibSemanticTypeClassifier, TrainingSettings}

case class ModelTrainerPaths(curModel: Model,
                             workspacePath: String,
                             featuresConfigPath: String,
                             costMatrixConfigPath: String,
                             labelsDirPath: String,
                             featureExtractorPath: String = "",
                             pipelinePath: String = "")

case class DataintTrainModel(classes: List[String],
                             trainingSet: DataModel,
                             labels: SemanticTypeLabels,
                             trainSettings: TrainingSettings,
                             postProcessingConfig: Option[Map[String, Any]])

object ModelTrainer extends LazyLogging with MatcherJsonFormats {

  protected val rootDir: String = ModelStorage.rootDir

  /**
    * Return an instance of class TrainingSettings
    */
  protected def readSettings(trainerPaths: ModelTrainerPaths): TrainingSettings = {

    val featuresConfig = FeatureSettings.load(
      trainerPaths.featuresConfigPath,
      trainerPaths.workspacePath)

    TrainingSettings(
      resamplingStrategy = trainerPaths.curModel.resamplingStrategy.str,
      featureSettings = featuresConfig,
      costMatrix = Some(Left(trainerPaths.costMatrixConfigPath)),
      numBags = trainerPaths.curModel.numBags,
      bagSize = trainerPaths.curModel.bagSize
    )
  }

  /**
    * Returns a list of DataModel instances at path
    */
  protected def getDataModels: List[DataModel] = {
    DatasetStorage
      .getCSVResources
      .map(CSVDataLoader().load)
  }

  /**
    * Returns a list of DataModel instances for the dataset repository
    */
  protected def readTrainingData: DataModel = {

    logger.info(s"Reading training data...")

    val datasets = getDataModels

    if (datasets.isEmpty) { // training dataset has to be non-empty
      logger.error("No csv training datasets have been found.")
      throw NotFoundException("No csv training datasets have been found.")
    }

    logger.info(s"    training data read!")
    new DataModel("", None, None, Some(datasets))
  }

  /**
    * Reads in labeled data
    */
  protected def readLabeledData(trainerPaths: ModelTrainerPaths): SemanticTypeLabels = {

    logger.info(s"Reading label data... ")

    val labelsLoader = SemanticTypeLabelsLoader()
    val stl = labelsLoader.load(trainerPaths.labelsDirPath)

    if (stl.labelsMap.isEmpty) {// we do not allow unsupervised setting; labeled data should not be empty
      logger.error("No labeled datasets have been found.")
      throw NotFoundException("No labeled datasets have been found.")
    }
    logger.info(s"    label data read!")
    stl
  }

  /**
    * This method writes JSON for featureExtractors which are generated during the training process.
    * @param id Model id
    * @param featureExtractors List of feature extractors: both single and group
    * @return
    */
  def writeFeatureExtractors(id: ModelID,
                             featureExtractors: List[FeatureExtractor]
                            ): Unit = {

    logger.info(s"Writing feature extractors to disk for model $id")
    Try {
      val modelFeatureExractors = ModelFeatureExtractors(featureExtractors)
      val str = compact(Extraction.decompose(modelFeatureExractors))
      val outputPath = Paths.get(ModelStorage.identifyPaths(id).get.featureExtractorPath)

      // ensure that the directories exist...
      val dir = outputPath.toFile.getParentFile
      if (!dir.exists) dir.mkdirs

      // write the object to the file system
      Files.write(
        outputPath,
        str.getBytes(StandardCharsets.UTF_8)
      )
      str
    } match {
      case Success(s) =>
        logger.info("Model feature extractors successfully written to disk.")
      case Failure(err) =>
        logger.info(s"Failed writing model feature extractors successfully to disk: $err")
        throw InternalException(s"Failed writing model feature extractors successfully to disk: $err")
    }

  }

  def writePipelineModel(id: ModelID, pipeModel: PipelineModel): Unit = {
    val writePath = Paths.get(ModelStorage.identifyPaths(id).get.pipelinePath)
    val out = Try(new ObjectOutputStream(new FileOutputStream(writePath.toString)))
    logger.info(s"Writing pipeline rf: $writePath")
    out match {
      case Failure(err) =>
        logger.error(s"Failed to write pipeline: ${err.getMessage}")
      case Success(f) =>
        f.writeObject(pipeModel)
        f.close()
    }
  }

  /**
    * Performs training for the model and returns serialized object for the learned model
    */
  def train(id: ModelID): Option[SerializableMLibClassifier] = {
    logger.info(s"    train called for model $id")
    ModelStorage.identifyPaths(id)
      .map(cts  => {
        if (!Files.exists(Paths.get(cts.workspacePath))) {
          val msg = s"Workspace directory for the model $id does not exist."
          logger.error(msg)
          throw NotFoundException(msg)
        }

        logger.info("Attempting to create training object..")

        val dataTrainModel = DataintTrainModel(
          classes = cts.curModel.classes,
          trainingSet = readTrainingData,
          labels = readLabeledData(cts),
          trainSettings = readSettings(cts),
          postProcessingConfig = None)

        logger.info(s"    created data training model!")

        dataTrainModel
      })
      .map(dt => {

        val trainer = TrainMlibSemanticTypeClassifier(dt.classes, doCrossValidation = false)

        val randomForestSchemaMatcher = trainer.train(
          dt.trainingSet,
          dt.labels,
          dt.trainSettings,
          dt.postProcessingConfig)

        writeFeatureExtractors(id, randomForestSchemaMatcher.featureExtractors)
        writePipelineModel(id, randomForestSchemaMatcher.model)
        logger.info(s"   feature names: ${FeatureExtractorUtil
          .getFeatureNames(randomForestSchemaMatcher.featureExtractors)}")
        logger.info(s"    feature importances: ${randomForestSchemaMatcher
          .model.stages(2)
          .asInstanceOf[RandomForestClassificationModel].featureImportances}")

        SerializableMLibClassifier(
          randomForestSchemaMatcher.model,
          dt.classes,
          randomForestSchemaMatcher.featureExtractors,
          randomForestSchemaMatcher.postProcessingConfig)
      })

  }

}

