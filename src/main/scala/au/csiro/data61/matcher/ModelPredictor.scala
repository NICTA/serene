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

import au.csiro.data61.matcher.api.{InternalException, NotFoundException}
import au.csiro.data61.matcher.storage.{DatasetStorage, ModelStorage}
import au.csiro.data61.matcher.types.ColumnPrediction
import au.csiro.data61.matcher.types.ColumnTypes._
import au.csiro.data61.matcher.types.DataSetTypes.DataSetID
import au.csiro.data61.matcher.types.ModelTypes.ModelID
import com.nicta.dataint.matcher.MLibSemanticTypeClassifier
import com.nicta.dataint.matcher.train.TrainAliases.PredictionObject
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FilenameUtils
import org.joda.time

import scala.io.Source
import scala.util.{Failure, Success, Try}

// data integration project
import com.nicta.dataint.ingestion.loader.CSVHierarchicalDataLoader
import com.nicta.dataint.matcher.serializable.SerializableMLibClassifier

object ModelPredictor extends LazyLogging {

  val rootDir: String = ModelStorage.rootDir
  val datasetDir: String = DatasetStorage.rootDir

  /**
    * Reads the file with the serialized MLib classifier and returns it.
    *
    * @param filePath string which indicates file location
    * @return Serialized Mlib classifier wrapped in Option
    */
  def readLearntModelFile(filePath: String) : Option[SerializableMLibClassifier] = {
    (for {
      learnt <- Try( new ObjectInputStream(new FileInputStream(filePath)))
        .orElse(Failure( new IOException("Error opening model file.")))
      data <- Try(learnt.readObject().asInstanceOf[SerializableMLibClassifier])
        .orElse(Failure( new IOException("Error reading model file.")))
    } yield data) match {
      case Success(mod) => Some(mod)
      case _ => None
    }
  }


  /**
    * Performs prediction for the model and returns predictions for all datasets in the repository
    *
    * @param id id of the model
    * @return Serialized Mlib classifier wrapped in Option
    */
  def predict(id: ModelID, datasetID: Option[DataSetID] = None): List[Option[PredictionObject]] = {
    // read in the learnt model
    val serialMod = readLearntModelFile(ModelStorage.modelPath(id).toString)
      .getOrElse(throw InternalException(s"Failed to read the learnt model for $id"))

    datasetID match {
      case Some(dsID) => {
        logger.info(s"Prediction for the dataset $dsID.")
        List(DatasetStorage.get(dsID)
          .map(_.path.toString)
          .filter(_.endsWith("csv"))
          .flatMap(predictDataset(id, _, serialMod)))
        // if datasetID does not exist or it is not csv, then nothing will be done
      }
      case None => {
        logger.info(s"Performing prediction for all datasets in the repository.")
        val preds = DatasetStorage
          .getCSVResources // get all csv files from data repository
          .map(predictDataset(id, _, serialMod)) // predict each dataset
        preds
      }
    }
  }

  /**
    * Performs prediction for a specified dataset using the model
    * and returns predictions for the specified dataset in the repository
    *
    * @param id id of the model
    * @param dsPath path of the dataset
    * @param sModel Serialized Mlib classifier
    * @return PredictionObject wrapped in Option
    */
  def predictDataset(id: ModelID
                     , dsPath: String
                     , sModel: SerializableMLibClassifier): Option[PredictionObject] = {
    val dsName = s"${FilenameUtils.getBaseName(dsPath)}.${FilenameUtils.getExtension(dsPath)}"
    val dataset = CSVHierarchicalDataLoader().readDataSet(Paths.get(dsPath).getParent.toString, dsName)
    val derivedFeatureFile = Paths.get(ModelStorage.getPredictionsPath(id).toString, dsName)

    val model = ModelStorage.get(id).getOrElse(throw NotFoundException(s"Model $id not found."))
    if (derivedFeatureFile.toFile.exists // file with predicted classes already exists
      && model.dateModified.isBefore(derivedFeatureFile.toFile.lastModified) // model was not modified after the file was created
      && model.state.dateModified.isBefore(derivedFeatureFile.toFile.lastModified) // model state was not modified after the file was created
      && Paths.get(dsPath).toFile.lastModified < derivedFeatureFile.toFile.lastModified) { // dataset was not modified after the file was created
      // prediction has been already done and is up to date
      logger.info(s"Prediction has been already done and is up to date for $dsPath.")
      None
    }
    else {
      val randomForestClassifier = MLibSemanticTypeClassifier(
        sModel.classes, sModel.model, sModel.featureExtractors, None, Option(derivedFeatureFile.toString))

      //Option(randomForestClassifier.predict(List(dataset)))
      Try(randomForestClassifier.predict(List(dataset))) match {
        case Success(preds) => Some(preds)
        case Failure(err) => {
          // prediction failed for the dataset
          logger.warn(s"Prediction for the dataset $dsPath failed: $err")
          None
          //throw InternalException(s"Prediction for dataset $dsPath failed: $err")
          // TODO: should we throw error or return just None for this dataset???
        }
      }
    }
  }

  /**
    * Read predictions from the csv file
    *
    * @param filePath string which indicates the location of the file with predictions
    * @param classNum number of classes in the model
    * @param modelID id of the model
    * @return List of ColumnPrediction
    */
  def readPredictions(filePath: String, classNum : Int, modelID: ModelID): List[ColumnPrediction] = {
    logger.info(s"Reading predictions from: $filePath...")
    (for {
      content <- Try(Source.fromFile(filePath).getLines.map(_.split(",")))
      // header: id, label, confidence, scores for classes, features
      header <- Try(content.next)
      classNames <- Try(header.slice(3, 3 + classNum))
      featureNames <- Try(header.slice(4 + classNum, header.size))
      preds <-  Try(content.map {
        case arr => {
          val dsID = FilenameUtils.getBaseName(arr(0).split("/")(0)) toInt
          val colID = DatasetStorage.columnNameMap(dsID, arr(0).split("/")(1)).id
          val label = arr(1)
          val confid = arr(2) toDouble
          val scores = classNames zip arr.slice(3, 3 + classNum).map(_.toDouble) toMap
          val featureVals = featureNames zip
            arr.slice(4 + classNum, arr.size).map(_.toDouble) toMap

          ColumnPrediction(modelID, dsID, colID, label, confid, scores, featureVals)
        }
      })
    } yield preds) match {
      case Success(predictions) => {
        val p = predictions.toList
        logger.info(s"${p.size} predictions have been read.")
        p
      }
      case Failure(err) =>
        logger.error(s"Failed reading predictions $filePath with error $err.")
        throw(InternalException(s"Failed reading predictions $filePath with error $err."))
    }
  }

  /**
    * Get predictions for the list of datasets if available.
    *
    * @param id id of the model
    * @param datasetIDs  list of ids of the datasets
    * @return List of ColumnPrediction
    */
  def getDatasetPrediction(id: ModelID, datasetIDs: List[DataSetID]): List[ColumnPrediction] = {
    logger.info(s"Attempting to read predictions for datasets: $datasetIDs")
    val predPath = ModelStorage.getPredictionsPath(id) // directory where prediction files are stored for this model
    // get number of classes for this model
    val numClasses = ModelStorage.get(id).map(_.classes.size).getOrElse(throw NotFoundException(s"Model $id not found."))
    // read in predictions for columns which are available in the prediction directory
    logger.info(s"Predictions are available for datasets: ${ModelStorage.availablePredictions(id)}")
    ModelStorage.availablePredictions(id)
      .filter(datasetIDs.contains(_)) // if columns from datasetIDs are not available, an empty list will be returned
      .map(dsID => Paths.get(predPath.toString, s"$dsID.csv").toString)
      .flatMap{readPredictions(_, numClasses, id)}
    // add some logging if dataset ids are not found???
  }

}
