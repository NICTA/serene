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
package au.csiro.data61.matcher.storage

import java.io._
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import au.csiro.data61.matcher.api.NotFoundException
import au.csiro.data61.matcher.types.DataSetTypes.DataSetID
import au.csiro.data61.matcher.{Config, ModelTrainerPaths}
import au.csiro.data61.matcher.types.ModelTypes.{Model, ModelID, Status, TrainState}
import com.github.tototoshi.csv.CSVWriter
import com.nicta.dataint.matcher.serializable.SerializableMLibClassifier
import org.apache.commons.io.{FileUtils, FilenameUtils}
import org.joda.time.DateTime
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

/**
  * Default filenames used in data integration code
  */
object DefaultFilenames {
  val CostMatrix = "cost_matrix_config.json"
  val FeaturesConfig = "features_config.json"
  val LabelOutDir = "labels"
  val Labels = "labels.csv"
  val LabelHeader = List("attr_id", "class")
  val WorkspaceDir = "workspace"
  val PredictionsDir = "predictions/"
}

/**
 * Object for storing models
 */
object ModelStorage extends Storage[ModelID, Model] {

  def rootDir: String = new File(Config.ModelStorageDir).getAbsolutePath

  def extract(stream: FileInputStream): Model = {
    parse(stream).extract[Model]
  }

  /**
    * Returns the path to the serialized trained model
    *
    * @param id The `id` key to the model
    * @return Path to the binary resource
    */
  def modelPath(id: ModelID): Path = {
    Paths.get(wsPath(id).toString, s"$id.rf")
  }

  /**
    * Returns the location of the workspace directory for id
    * For now it's relevant only for models
    *
    * @param id The ID for the Value
    * @return
    */
  protected def wsPath(id: ModelID): Path = {
    Paths.get(getDirectoryPath(id).toString, DefaultFilenames.WorkspaceDir)
  }

  /**
    * Returns the location of the predictions directory for id
    *
    * @param id The ID for the Value
    * @return
    */
  def predictionsPath(id: ModelID): Path = {
    Paths.get(wsPath(id).toString, DefaultFilenames.PredictionsDir)
  }

  /**
    * Attempts to read all the objects out from the storage dir
    *
    * Note that here we do a basic error check and reset all the
    * paused-state 'TRAINING' models back to untrained.
    *
    * @return
    */
  override def listValues: List[Model] = {
    super.listValues
      .map {
        case model =>
          if (model.state.status == Status.BUSY) {
            val newModel = model.copy(
              state = model.state.copy(status = Status.UNTRAINED)
            )
            update(model.id, newModel)
            newModel
          } else {
            model
          }
      }
  }

  /**
    * Transforms user provided label data to the old format
    *
    * @param value The Model to write to disk
    */
  def convertLabelData(value : Model) : List[List[String]] = {

    logger.debug("converting labelled data to schema matcher format")

    // header for the file
    val header = DefaultFilenames.LabelHeader

    // converting to the format: "dataSetID.csv/columnName,labelName"
    val body = value.labelData
      .map { case (id, label) =>

        // lookup column in columnMap and extract the data from there...
        val labelList = for {
          col <- DatasetStorage.columnMap.get(id)
          dsPath = col.path.getFileName
          dsName = s"$dsPath/${col.name}"
        } yield List(dsName, label)

        labelList getOrElse {

          logger.warn(s"Failed to get labels for id=$id label=$label")
          List.empty[String]
        }
      }.toList

    logger.debug(body.mkString("\n"))

    header :: body
  }


  /**
    * Writes the cost matrix from model to the wsDir
    *
    * @param wsDir The output workspace directory
    * @param model The model object
    * @param outFile The name of the output JSON file
    * @return Try containing the final output path
    */
  private def writeCostMatrix(wsDir: String,
                              model: Model,
                              outFile: String = DefaultFilenames.CostMatrix): Try[String] = {
    Try {
      val costMatrixConfigPath = Paths.get(wsDir.toString, outFile)
      val strCostMatrix = compact(Extraction.decompose(model.costMatrix))

      logger.info(s"Writing cost matrix for model ${model.id} to $costMatrixConfigPath")

      Files.write(
        costMatrixConfigPath,
        strCostMatrix.getBytes(StandardCharsets.UTF_8)
      )
      costMatrixConfigPath.toString
    }
  }

  /**
    * Writes the features config file to the workspace
    *
    * @param wsDir The output directory
    * @param model The model object
    * @param outFile The name of the output JSON file
    * @return Try containing the final output path
    */
  private def writeFeaturesConfig(wsDir: String,
                                  model: Model,
                                  outFile: String = DefaultFilenames.FeaturesConfig): Try[String] = {
    Try {
      val featuresConfigPath = Paths.get(wsDir.toString, outFile)
      val strFeatures = compact(Extraction.decompose(model.features))

      logger.info(s"Writing feature config file for model ${model.id} at $featuresConfigPath")

      Files.write(
        featuresConfigPath,
        strFeatures.getBytes(StandardCharsets.UTF_8)
      )

      featuresConfigPath.toString
    }
  }

  /**
    * Writes the label information to the workspace directory
    *
    * TODO: type-map is part of  featureExtractorParams, type-mas need to be read from datasetrepository when model gets created
    *  "featureExtractorParams": [{"name": "inferred-data-type","type-map": "src/test/resources/config/type_map.csv"}]
    *
    * labels; we want to write csv file with the following content:
    * attr_id,class
    * datasetID.csv/columnName,labelName
    *
    * @param wsDir The output workspace
    * @param model The model object
    * @param outDir The output subdirectory for the labels
    * @param outFile The output filename
    * @return
    */
  private def writeLabels(wsDir: String,
                          model: Model,
                          outDir: String = DefaultFilenames.LabelOutDir,
                          outFile: String = DefaultFilenames.Labels): Try[String] = {
    Try {
      val labelsDir = Paths.get(wsDir, outDir)
      if (!labelsDir.toFile.exists) {
        labelsDir.toFile.mkdirs
      }
      val labelsPath = Paths.get(labelsDir.toString, outFile).toString
      val labelData = convertLabelData(model)

      logger.info(s"Writing labels for model ${model.id} to $labelsPath")

      val writer = CSVWriter.open(labelsPath)
      writer.writeAll(labelData)
      writer.close()

      labelsPath
    }
  }

  /**
    * Writes the object to disk as a serialized json string
    * at a pre-defined location based on the id. The config
    * files written are done so according to the data integration
    * project folder specs.
    *
    * @param model The Model to write to disk
    */
  override protected def writeToFile(model: Model): Unit = {

    logger.debug(s"Writing model ${model.id} to file.")

    // write model json
    super.writeToFile(model)

    // write config files according to the data integration project...
    // workspace directory
    val wsDir = wsPath(model.id).toFile
    val wsDirStr = wsDir.toString

    // create workspace directory if it doesn't exist
    if (!wsDir.exists) {
      wsDir.mkdirs
    }

    // write predictions directory
    val predDir = predictionsPath(model.id).toFile
    if (!predDir.exists) {
      predDir.mkdirs
    }

    // extract amd write
    val writeStatus = for {
      cm <- writeCostMatrix(wsDirStr, model)
      fc <- writeFeaturesConfig (wsDirStr, model)
      labels <- writeLabels (wsDirStr, model)
    } yield (cm, fc, labels)

    writeStatus match {
      case Success(_) =>
        logger.info(s"Model ${model.id} written successfully to workspace.")
      case Failure(err) =>
        logger.error(s"Failed to write model ${model.id} to workspace.")
        throw new Exception(err.getMessage)
    }
  }

  /**
    * Updates the model at `id` and also deletes the previously
    * trained model if it exists.
    * The previously trained model should not be deleted if training was a success!!!
    *
    * @param id       ID to give to the element
    * @param value    Value object
    * @param deleteRF Boolean which indicates whether the trained model file should be deleted
    * @return ID of the resource created (if any)
    */
  override def update(id: ModelID, value: Model, deleteRF: Boolean = true): Option[ModelID] = {
    for {
      updatedID <- super.update(id, value)
      deleteOK <- deleteRF match {
        case false => Some(id) // if the model has been successfully trained or we're doing prediction, model file should not be deleted
        case true => deleteModel(updatedID)
      }
    } yield deleteOK
  }

  /**
    * Deletes the model file resource if available
    *
    * @param id The key for the model object
    * @return
    */
  protected def deleteModel(id: ModelID): Option[ModelID] = {
    cache.get(id) match {
      case Some(ds) =>
        val modelFile = modelPath(id)

        if (Files.exists(modelFile)) {
          // delete model file - be careful
          synchronized {
            Try(FileUtils.deleteQuietly(modelFile.toFile)) match {
              case Failure(err) =>
                logger.error(s"Failed to delete file: ${err.getMessage}")
                None
              case _ =>
                Some(id)
            }
          }
        } else {
          Some(id)
        }
      case _ =>
        logger.error(s"Resource not found: $id")
        None
    }
  }

  /**
    * Writes the MLib classifier object to a file at address `id`
    *
    * @param id          The id key for the model
    * @param learntModel The trained model
    * @return
    */
  def writeModel(id: ModelID, learntModel: SerializableMLibClassifier): Boolean = {
    val writePath = modelPath(id).toString

    val out = Try(new ObjectOutputStream(new FileOutputStream(writePath)))
    logger.info(s"Writing model rf:  $writePath")
    out match {
      case Failure(err) =>
        logger.error(s"Failed to write model: ${err.getMessage}")
        false
      case Success(f) =>
        f.writeObject(learntModel)
        f.close()
        true
    }
  }

  /**
    * updates the training state of model `id`
    *
    * Note that when we update, we need to keep the model level 'dateModified' to
    * ensure that the model parameters remains static for dataset comparisons.
    *
    * @param id     The key for the model
    * @param status The current status of the model training.
    * @return
    */
  def updateTrainState(id: ModelID
                       , status: Status
                       , msg: String = ""
                       , deleteRF: Boolean = true
                       , changeDate: Boolean = true): Option[TrainState] = {
    synchronized {
      for {
        model <- ModelStorage.get(id)
        // state dates should not be changed if changeDate is false
        trainState = if (changeDate) {
          TrainState(status, msg, model.state.dateCreated, DateTime.now)
        }
        else {
          TrainState(status, msg, model.state.dateCreated, model.state.dateModified)
        }
        id <- ModelStorage.update(id
          , model.copy(state = trainState, dateModified = model.dateModified)
          , deleteRF)
      } yield trainState
    }
  }

  /**
    * Identify paths which are needed to train the model at id
    *
    * @param id
    * @return
    */
  def identifyPaths(id: ModelID): Option[ModelTrainerPaths] = {
    val wsDir = wsPath(id).toString
    logger.info(s"Identifying paths for the model $id")
    ModelStorage.get(id)
      .map(cm =>
        ModelTrainerPaths(curModel = cm,
          workspacePath = wsDir,
          featuresConfigPath = Paths.get(wsDir, DefaultFilenames.FeaturesConfig).toString,
          costMatrixConfigPath = Paths.get(wsDir, DefaultFilenames.CostMatrix).toString,
          labelsDirPath = Paths.get(wsDir, DefaultFilenames.LabelOutDir).toString))
  }

  /**
    * Check if the file for the trained model was written after changes to the model
    *
    * @param model
    * @param modelFile
    * @return boolean
    */
  def checkModelFileCreation(model: Model, modelFile: String): Boolean = {
    val f = new File(modelFile)
    val stateLastModified = model.state.dateModified
    (f.exists // model.rf file exists
      && model.dateModified.isBefore(f.lastModified) // model.rf was modified after model modifications
      && model.dateModified.isBefore(stateLastModified) // state was modified after model modifications
      )
  }

  /**
    * Check if the model was trained after changes to the dataset repository
    *
    * @param model
    * @return boolean
    */
  def checkModelTrainDataset(model: Model): Boolean = {
    model.refDataSets
      .map(DatasetStorage.get)
      .map {
        case Some(ds) =>
          ds.dateModified.isBefore(model.state.dateModified)
        case _ =>
          false
      }
      .reduce(_ && _)
  }

  /**
    * Check if the trained model is consistent.
    * If the model is untrained or any error is encountered, it returns false.
    * If the learnt model is consistent, it returns true.
    *
    * @param id
    * @return boolean
    */
  def isConsistent(id: ModelID): Boolean = {
    // check if .rf file exists
    // check model state
    // check if json file was updated after .rf file was created
    // check if dataset repo was updated
    logger.info(s"Checking consistency of model $id")
    val modelFile = modelPath(id).toString
    ModelStorage.get(id) match {
      case Some(model) =>
        (checkModelFileCreation(model, modelFile)
          && checkModelTrainDataset(model)
          && model.state.status == Status.COMPLETE)
      case _ =>
        false // model does not exist or some other problem
    }
  }

  /**
    * List those dataset ids for which up-to-date predictions are available.
    *
    * @param id Model id
    * @return List of strings which indicate files with calculated predictions.
    */
  def predictionCache(id: ModelID): List[DataSetID] = {

    val model = ModelStorage.get(id).getOrElse(throw NotFoundException(s"Model $id not found."))

    val predPath = predictionsPath(id)

    Option(new File(predPath.toString) listFiles) match {
      case Some(fileList) =>
        val files = fileList
          .filter(_.isFile)
          .filter(x =>
            // get only those predictions which are up to date
            (model.dateModified.isBefore(x.lastModified)
              && model.state.dateModified.isBefore(x.lastModified)))

        files
          // checking model state should be unneccessary
          // we need to check if datasets have been changed
          .map(predFile => (predFile, predFile.toString))
          .map {
              case (file, str) =>
                (file, FilenameUtils.getBaseName(str))
          }
          //.toList
          .map { case (file, str) =>
            (file, Try(str.toInt).toOption)
          }   // converting strings to integers
          .flatMap {
            case (predFile, Some(dsKey)) =>
              Some((predFile, DatasetStorage.get(dsKey)))
            case _ =>
              None
          }
          .filter {
            case (predFile, Some(dataset)) =>
              dataset.dateModified.isBefore(predFile.lastModified) // removing those predictions for which datasets have been modified
            case _ =>
              false
          }
          .flatMap {
            case (predFile, Some(dataset)) =>
              Some(dataset.id)
            case _ =>
              None
          }.toList
      case _ =>
        logger.warn(s"Failed to open predictions dir ${predPath.toString}.")
        List.empty[DataSetID]
    }
  }
}
