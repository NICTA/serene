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
import au.csiro.data61.core.{Config, Serene}
import au.csiro.data61.core.storage.{DatasetStorage, JsonFormats, MatcherConstants, ModelStorage}
import au.csiro.data61.types.DataSetTypes.DataSetID
import au.csiro.data61.types.ModelTypes.{Model, ModelID}
import au.csiro.data61.matcher.matcher.MLibSemanticTypeClassifier
import au.csiro.data61.matcher.matcher.features._
import au.csiro.data61.matcher.matcher.featureserialize.ModelFeatureExtractors
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.classification.RandomForestClassificationModel
import org.apache.spark.ml.linalg.{DenseVector, Vector}
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.language.postfixOps
import scala.util.{Failure, Success, Try}


// data integration project
import au.csiro.data61.matcher.data.{DataModel, SemanticTypeLabels}
import au.csiro.data61.matcher.ingestion.loader.{CsvDataLoader, SemanticTypeLabelsLoader}
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

object ModelTrainer extends LazyLogging with JsonFormats {

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
      numBags = Some(trainerPaths.curModel.numBags),
      bagSize = Some(trainerPaths.curModel.bagSize)
    )
  }

  /**
    * Returns a list of DataModel instances at path
    */
  protected def getDataModels(datasets: List[DataSetID]): List[DataModel] = {
    DatasetStorage
      .getCSVResources(datasets)
      .map(CsvDataLoader().load)
  }

  /**
    * Returns a list of DataModel instances for the dataset repository
    */
  protected def readTrainingData(datasetIds: List[DataSetID]): DataModel = {

    logger.info(s"Reading training data...")

    val datasets = getDataModels(datasetIds)

    logger.info(s"Read ${datasets.length} data models.")

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
    *
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

  /**
    * Write Spark pipeline model.
    *
    * @param id id of the model
    * @param pipeModel Spark pipelinemodel
    */
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
    * Write feature importances for the model to the file.
    *
    * @param id model id
    * @param randomForestSchemaMatcher Trained model returned by schema matcher code
    */
  def writeFeatureImportances(id: ModelID,
                              randomForestSchemaMatcher: MLibSemanticTypeClassifier
                             ): Unit = {
    logger.info(s"Writing feature importances to file for model $id")
    Try {
      // feature importances returned by Spark random forest classifier -- we use now gini
      val fimp = randomForestSchemaMatcher
        .model.stages(2)
        .asInstanceOf[RandomForestClassificationModel]
        .featureImportances.toDense
      // names of features which were extracted for training
      val names: List[String] = FeatureExtractorUtil
        .getFeatureNames(randomForestSchemaMatcher.featureExtractors)

      // the number of feature names should be the same as the number of calculated feature importances
      assert(fimp.size == names.size)

      val conv: List[(String, Double)] = names
        .zip(fimp.toArray)
        .sortBy(_._2)(Ordering[Double].reverse)

      val path = Paths.get(ModelStorage.identifyPaths(id).get.workspacePath,
        MatcherConstants.FeatureImportanceFile).toString
      val out = new PrintWriter(new File(path))

      val header = "feature_name,importance"
      out.println(header)

      //write features
      conv.foreach {
        case (f: String, imp: Double) =>
          out.println(s"$f,$imp")
      }
      out.close()
      path
    } match {
      case Success(filename) =>
        logger.info(s"Feature importances have been written to $filename .")
      case Failure(err) =>
        logger.warn(s"Failed writing feature importances to file.")
    }

  }

  /**
    * Performs training for the model and returns serialized object for the learned model
 *
    * @param id The model ID to train
    * @return
    */
  def train(id: ModelID): Option[SerializableMLibClassifier] = {
    logger.info(s"    train called for model $id")
    ModelStorage.identifyPaths(id)
      .flatMap(cts  => {

        if (!Files.exists(Paths.get(cts.workspacePath))) {
          val msg = s"Workspace directory for the model $id does not exist."
          logger.error(msg)
          throw NotFoundException(msg)
        }
        logger.info("Attempting to create training object..")

        createTrainModel(id, cts)
      })
      .map(createMLibClassifier(id, _))
  }

  /**
    * Creates the DataintTrainModel object
 *
    * @param id The model ID to train
    * @param cts The model training paths
    * @return
    */
  protected def createTrainModel(id: ModelID, cts: ModelTrainerPaths): Option[DataintTrainModel] = {
    val dataTrainModelOpt = for {
      model <- ModelStorage.get(id)

      columnKeys = model.labelData.keys.toSet

      columns = DatasetStorage.columnMap.filterKeys(columnKeys.contains).values

      datasets = columns.map(_.datasetID).toList

      dataTrainModel = DataintTrainModel(
        classes = cts.curModel.classes,
        trainingSet = readTrainingData(datasets),
        labels = readLabeledData(cts),
        trainSettings = readSettings(cts),
        postProcessingConfig = None)

    } yield dataTrainModel

    logger.info(s"    created data training model!")
    dataTrainModelOpt
  }

  /**
    * Creates the SerializableMLibClassifier
    * @param id The ModelID
    * @param dt The trained model object
    * @return
    */
  protected def createMLibClassifier(id: ModelID, dt: DataintTrainModel): SerializableMLibClassifier = {

    val trainer = TrainMlibSemanticTypeClassifier(dt.classes, doCrossValidation = false)

    val randomForestSchemaMatcher = trainer.train(
      dt.trainingSet,
      dt.labels,
      dt.trainSettings,
      dt.postProcessingConfig,
      numWorkers = Serene.config.numWorkers,
      parallelFeatureExtraction = Serene.config.parallelFeatureExtraction)

    writeFeatureImportances(id, randomForestSchemaMatcher)

    SerializableMLibClassifier(
      randomForestSchemaMatcher.model,
      dt.classes,
      randomForestSchemaMatcher.featureExtractors,
      randomForestSchemaMatcher.postProcessingConfig)
  }

}

