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
import com.nicta.dataint.matcher.serializable.SerializableMLibClassifier
import org.apache.commons.io.{FileUtils, FilenameUtils}
import org.joda.time.{DateTime, DateTimeComparator}
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.language.postfixOps
import scala.util.{Failure, Success, Try}


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
    Paths.get(getWSPath(id).toString, s"$id.rf")
  }

  /**
    * Returns the location of the workspace directory for id
    * For now it's relevant only for models
    *
    * @param id The ID for the Value
    * @return
    */
  protected def getWSPath(id: ModelID): Path = {
    Paths.get(getDirectoryPath(id).toString, "workspace")
  }

  /**
    * Returns the location of the predictions directory for id
    *
    * @param id The ID for the Value
    * @return
    */
  def getPredictionsPath(id: ModelID): Path = {
    Paths.get(getWSPath(id).toString, "predictions/")
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
  def convertLabelData(value: Model): List[List[String]] = {
    //should we fail the splitting of files if some columns from labelData are not found in colunmMap???
    List("attr_id", "class") :: // header for the file
      value.labelData // converting to the format: "datasetID.csv/columnName,labelName"
        .map { x => {
        val col = DatasetStorage.columnMap(x._1) // lookup column in columnMap
        val ext = FilenameUtils.getExtension(col.path.toString).toLowerCase
        val dsWithExt = s"${col.datasetID}.$ext" // dataset name as it is stored in DatasetStorage
        List(s"$dsWithExt/${col.name}", x._2)
      }
      }
        .toList
    //    this is the way to do it if we want to check that lookups happen correctly
    //    val labelData : List[(String,String)] = value.labelData
    //      .map { x => {
    //        val column = Try(DatasetStorage.columnMap(x._1))
    //        column match{
    //          case Success(col) => (col.name, x._2)
    //          case _ => ("","")
    //        }}
    //      }
    //      .toList
    //      .filter(_ != ("",""))
  }

  /**
    * Writes the object to disk as a serialized json string
    * at a pre-defined location based on the id.
    *
    * @param value The Model to write to disk
    */
  override protected def writeToFile(value: Model): Unit = {

    super.writeToFile(value) // write model json

    //  write config files according to the data integration project
    val wsDir = getWSPath(value.id).toFile // workspace directory
    if (!wsDir.exists) wsDir.mkdirs // create workspace directory if it doesn't exist

    val predDir = getPredictionsPath(value.id).toFile // predictions directory
    if (!predDir.exists) predDir.mkdirs // create predictions directory if it doesn't exist

    //cost_matrix_config.json
    val costMatrixConfigPath = Paths.get(wsDir.toString, s"cost_matrix_config.json")
    val strCostMatrix = compact(Extraction.decompose(value.costMatrix))
    logger.info(s"Writing cost_matrix_config.json for model ${value.id}")
    Files.write(
      costMatrixConfigPath,
      strCostMatrix.getBytes(StandardCharsets.UTF_8)
    )

    //features_config.json
    val featuresConfigPath = Paths.get(wsDir.toString, "features_config.json")
    val strFeatures = compact(Extraction.decompose(value.features))
    logger.info(s"Writing features_config.json for model ${value.id}")
    Files.write(
      featuresConfigPath,
      strFeatures.getBytes(StandardCharsets.UTF_8)
    )

    //type_map.csv???
    // TODO: type-map is part of  featureExtractorParams, type-maps need to be read from dataset repository when model gets created
    // "featureExtractorParams": [{"name": "inferred-data-type","type-map": "src/test/resources/config/type_map.csv"}]

    //labels; we want to write csv file with the following content:
    // attr_id,class
    // columnName@datasetID.csv,labelName
    val labelsDir = Paths.get(wsDir.toString, s"labels")
    if (!labelsDir.toFile.exists) labelsDir.toFile.mkdirs
    val labelsPath = Paths.get(labelsDir.toString, s"labels.csv")
    val labelData = convertLabelData(value)
    logger.info(s"Writing labels.csv for model ${value.id}")
    val out = new PrintWriter(new File(labelsPath.toString))
    labelData.foreach(line => out.println(line.mkString(",")))
    out.close()

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
                       , deleteRF: Boolean = true): Option[TrainState] = {
    synchronized {
      for {
        model <- ModelStorage.get(id)
        trainState = TrainState(status, msg, model.state.dateCreated, DateTime.now)
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
    val wsDir = getWSPath(id).toString
    logger.info(s"Identifying paths for the model $id")
    ModelStorage.get(id)
      .map(cm =>
        ModelTrainerPaths(curModel = cm,
          workspacePath = wsDir,
          featuresConfigPath = Paths.get(wsDir, "features_config.json").toString,
          costMatrixConfigPath = Paths.get(wsDir, s"cost_matrix_config.json").toString,
          labelsDirPath = Paths.get(wsDir, s"labels").toString))
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
      .map(DatasetStorage.get(_))
      .map {
        case Some(ds) => ds.dateModified.isBefore(model.state.dateModified)
        case _ => false
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
      case Some(model) => {
        (checkModelFileCreation(model, modelFile)
          && checkModelTrainDataset(model)
          && model.state.status == Status.COMPLETE)
      }
      case _ => false // model does not exist or some other problem
    }
  }

  /**
    * List those dataset ids for which up to date predictions are available.
    *
    * @param id Model id
    * @return List of strings which indicate files with calculated predictions.
    */
  def availablePredictions(id: ModelID): List[DataSetID] = {
    val model = ModelStorage.get(id).getOrElse(throw NotFoundException(s"Model $id not found."))
    val predPath = getPredictionsPath(id)

    Option(new File(predPath.toString) listFiles) match {
      case Some(fileList) => {
        fileList
          .filter(_.isDirectory)
          .filter(x => model.dateModified.isBefore(x.lastModified)) // get only those predictions which are up to date, check also model.rf!!!
          .map(_.toString)
          .toList
          .flatMap(toKeyOption)   // converting strings to integers
      }
      case _ =>
        logger.error(s"Failed to open predictions dir ${predPath.toString}")
        List.empty[DataSetID]
    }
  }
}
