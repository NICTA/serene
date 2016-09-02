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

import java.io.{File, FileInputStream, IOException, ObjectInputStream}
import java.nio.file.Paths

import au.csiro.data61.matcher.api.{InternalException, NotFoundException}
import au.csiro.data61.matcher.storage.{DatasetStorage, ModelStorage}
import au.csiro.data61.matcher.types.ColumnPrediction
import au.csiro.data61.matcher.types.DataSetTypes.DataSetID
import au.csiro.data61.matcher.types.ModelTypes.{Model, ModelID}
import com.nicta.dataint.matcher.MLibSemanticTypeClassifier
import com.nicta.dataint.matcher.train.TrainAliases.PredictionObject
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FilenameUtils

import scala.io.Source
import scala.util.{Failure, Success, Try}

// data integration project
import com.nicta.dataint.ingestion.loader.CSVHierarchicalDataLoader
import com.nicta.dataint.matcher.serializable.SerializableMLibClassifier

import language.postfixOps

object ModelPredictor extends LazyLogging {

  val rootDir: String = ModelStorage.rootDir
  val datasetDir: String = DatasetStorage.rootDir

  /**
    * Reads the file with the serialized MLib classifier and returns it.
    *
    * @param filePath string which indicates file location
    * @return Serialized Mlib classifier wrapped in Option
    */
  def readLearnedModelFile(filePath: String) : Option[SerializableMLibClassifier] = {
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
  def predict(id: ModelID, datasetID: DataSetID): List[ColumnPrediction] = {

    logger.info(s"Prediction for the dataset $datasetID.")

    if (ModelStorage.predictionCache(id).contains(datasetID)) {
      getCachedPrediction(id, datasetID)
    } else {
      // read in the learned model
      val serialMod = readLearnedModelFile(ModelStorage.modelPath(id).toString)
        .getOrElse(throw InternalException(s"Failed to read the learned model for $id"))

      // if datasetID does not exist or it is not csv, then nothing will be done
      DatasetStorage
        .get(datasetID)
        .map(_.path.toString)
        .filter(_.endsWith("csv"))
        .flatMap(runPrediction(id, _, serialMod, datasetID))
        .getOrElse { throw InternalException("Failed to predict model") }
    }
  }

  /**
    * Get predictions for the list of datasets if available.
    *
    * @param id id of the model
    * @param datasetID  list of ids of the datasets
    * @return List of ColumnPrediction
    */
  def getCachedPrediction(id: ModelID, datasetID: DataSetID): List[ColumnPrediction] = {

    logger.info(s"Attempting to read predictions for dataset: $datasetID")

    // directory where prediction files are stored for this model
    val predPath = ModelStorage.predictionsPath(id)

    // get number of classes for this model
    val numClasses = ModelStorage.get(id)
      .map(_.classes.size)
      .getOrElse(throw NotFoundException(s"Model $id not found."))

    // read in predictions for columns which are available in the prediction directory
    logger.info(s"Predictions are available for datasets: ${ModelStorage.predictionCache(id)}")

//    ModelStorage
//      .predictionCache(id)
//      //.filter(datasetID.contains) // if columns from datasetIDs are not available, an empty list will be returned
//      .map(dsID => Paths.get(predPath.toString, s"$dsID.csv").toString)
//      .flatMap {
//        readPredictions(_, numClasses, id)
//      }
    // add some logging if dataset ids are not found???
    readPredictions(Paths.get(predPath.toString, s"$datasetID.csv").toString, numClasses, id)
  }

/**
  * isValid checks to see if the model file is valid and up-to-date
  *
  * file with predicted classes already exists
  * and model was not modified after the file was created
  * and model state was not modified after the file was created
  * and dataset was not modified after the file was created
  */
  private def isValid(f: File, model: Model, path: String): Boolean = {
    (f.exists
      && model.dateModified.isBefore(f.lastModified)
      && model.state.dateModified.isBefore(f.lastModified)
      && Paths.get(path).toFile.lastModified < f.lastModified)
  }

  /**
    * Performs prediction for a specified dataset using the model
    * and returns predictions for the specified dataset in the repository
    *
    * @param id id of the model
    * @param dsPath path of the dataset
    * @param sModel Serialized Mlib classifier
    * @param datasetID id of the dataset
    * @return PredictionObject wrapped in Option
    */
  def runPrediction(id: ModelID,
                    dsPath: String,
                    sModel: SerializableMLibClassifier,
                    datasetID: DataSetID): Option[List[ColumnPrediction]] = {

    // name of the derivedFeatureFile
    val writeName = s"$datasetID.csv"

    // loading data in the format suitable for data-integration project
    val dataset = CSVHierarchicalDataLoader().readDataSet(
      FilenameUtils.getFullPath(dsPath), //Paths.get(dsPath).getParent.toString,
      FilenameUtils.getName(dsPath)
    )

    // this is the file where predictions will be written
    val derivedFeatureFile = Paths.get(ModelStorage.predictionsPath(id).toString, writeName)

    val model = ModelStorage.get(id)
      .getOrElse(throw NotFoundException(s"Model $id not found."))

    val f = derivedFeatureFile.toFile

    if (isValid(f, model, dsPath)) {
      // prediction has been already done and is up to date
      logger.info(s"Prediction has been already done and is up to date for $dsPath.")
      None
    }
    else {
      val randomForestClassifier = MLibSemanticTypeClassifier(
        sModel.classes,
        sModel.model,
        sModel.featureExtractors,
        None,
        Option(derivedFeatureFile.toString))

      Try(randomForestClassifier.predict(List(dataset))) match {
        case Success(preds) =>
          // TODO: convert PredictionObject to ColumnPrediction
          Try {
            readPredictions(derivedFeatureFile.toString, sModel.classes.size, id)
          } toOption
        case Failure(err) =>
          // prediction failed for the dataset
          logger.warn(s"Prediction for the dataset $dsPath failed: $err")
          None
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
      preds <- Try(content.map {
        case arr =>
          val dsID = FilenameUtils.getBaseName(filePath) toInt
          // here we assume that attribute names contain fileNames
          //that's why we use filePath to get datasetID
          val colID = DatasetStorage.columnNameMap(dsID, arr(0).split("/")(1)).id
          val label = arr(1)
          val confid = arr(2) toDouble
          val scores = classNames zip arr.slice(3, 3 + classNum).map(_.toDouble) toMap
          val featureVals = featureNames zip
            arr.slice(4 + classNum, arr.size).map(_.toDouble) toMap

          ColumnPrediction(modelID, dsID, colID, label, confid, scores, featureVals)
      })
    } yield preds) match {
      case Success(predictions) =>
        val p = predictions.toList
        logger.info(s"${p.size} predictions have been read.")
        p
      case Failure(err) =>
        logger.error(s"Failed reading predictions $filePath with error $err.")
        throw InternalException(s"Failed reading predictions $filePath with error $err.")
    }
  }


}
