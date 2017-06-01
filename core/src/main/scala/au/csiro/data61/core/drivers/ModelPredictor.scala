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

import java.io.{FileInputStream, FileReader, ObjectInputStream}
import java.nio.file.{Path, Paths}

import au.csiro.data61.core.api.InternalException
import au.csiro.data61.core.storage.{DatasetStorage, JsonFormats, ModelStorage}
import au.csiro.data61.types.DataSetTypes.DataSetID
import au.csiro.data61.types.ModelTypes.ModelID
import au.csiro.data61.types._
import au.csiro.data61.matcher.ingestion.loader.CsvDataLoader
import au.csiro.data61.matcher.matcher.MLibSemanticTypeClassifier
import au.csiro.data61.matcher.matcher.features.FeatureExtractor
import au.csiro.data61.matcher.matcher.featureserialize.ModelFeatureExtractors
import au.csiro.data61.matcher.matcher.train.BaggingParams
import au.csiro.data61.matcher.matcher.train.TrainAliases._
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.csv.CSVFormat

import scala.util.{Failure, Success, Try}
import org.apache.spark.ml.PipelineModel
import org.json4s.jackson.JsonMethods._
import org.scalatest.path
import org.json4s._

import scala.collection.JavaConverters._
//import com.github.tototoshi.csv.CSVReader
// data integration project
import au.csiro.data61.matcher.ingestion.loader.CSVHierarchicalDataLoader
import au.csiro.data61.matcher.matcher.serializable.SerializableMLibClassifier

import scala.language.postfixOps


class ObjectInputStreamWithCustomClassLoader(fileInputStream: FileInputStream)
  extends ObjectInputStream(fileInputStream) {
  /**
    * This is a special deserialization for custom objects.
    * Either this custom thing, or fork := true in sbt are needed!
 *
    * @param desc
    * @return
    */
  override def resolveClass(desc: java.io.ObjectStreamClass): Class[_] = {
    try { Class.forName(desc.getName, false, getClass.getClassLoader) }
    catch { case ex: ClassNotFoundException => super.resolveClass(desc) }
  }
}


object ModelPredictor extends LazyLogging with JsonFormats {

  /**
    * Performs prediction for the model and returns predictions for a dataset in the repository
    * @param id id of the model
    * @return Serialized Mlib classifier wrapped in Option
    */
  def predict(id: ModelID, datasetID: DataSetID): DataSetPrediction = {
    logger.info(s"Dataset $datasetID is not in the cache. Computing prediction...")

    val serializedModel =
      for {
        // read in the learned model
        stored <- ModelStorage.get(id)

        path <- stored.modelPath

        m <- readLearnedModelFile(path.toString).toOption

      } yield m

    val sModel = serializedModel getOrElse {
      logger.error(s"Failed to read serialized model $id")
      throw InternalException(s"Failed to read serialized model $id")
    }

    logger.info(s"    serialized model has been read")
    // if datasetID does not exist or it is not csv, then nothing will be done
    DatasetStorage
      .get(datasetID)
      .filter(_.path.toString.toLowerCase.endsWith("csv"))
      .flatMap(ds => runPrediction(id, ds.path, sModel, datasetID))
      .getOrElse {
        throw InternalException("Failed to predict model")
      }
  }

  /**
    * Reads the file with the serialized MLib classifier and returns it.
    * @param filePath string which indicates file location
    * @return Serialized Mlib classifier wrapped in Option
    */
  protected def readLearnedModelFile(filePath: String) : Try[SerializableMLibClassifier] = {
    logger.info(s"Reading learned model file $filePath")
    for {
      fs <- Try {
        new FileInputStream(filePath)
      }
      learned = new ObjectInputStreamWithCustomClassLoader(fs)
      data <- Try {
        learned.readObject.asInstanceOf[SerializableMLibClassifier]
      }
    } yield data
  }

  def predictionsPath(modelID: ModelID, dataSetID: DataSetID): Path = {
    // name of the derivedFeatureFile
    val writeName = s"$dataSetID.csv"

    val predPath = ModelStorage.defaultPredictionsPath(modelID).toString

    // this is the file where predictions will be written
    Paths.get(predPath, writeName)
  }

  /**
    * Get bagging parameters depending on resampling strategy
    * @param id model id
    * @return
    */
  private def getBaggingParams(id: ModelID): Option[BaggingParams] = {
    logger.debug(s"Obtaining bagging params for prediction with model $id")
    for {
      stored <- ModelStorage.get(id)

      bnum <- Try {stored.numBags} toOption

      bsize <- Try {stored.bagSize} toOption

      strat <- stored.resamplingStrategy match {
          case SamplingStrategy.BAGGING => Some(BaggingParams(numBags = bnum, bagSize = bsize))
          case SamplingStrategy.BAGGING_TO_MAX => Some(BaggingParams(numBags = bnum, bagSize = bsize))
          case SamplingStrategy.BAGGING_TO_MEAN => Some(BaggingParams(numBags = bnum, bagSize = bsize))
          case _ => None
      }

    } yield strat
  }

  /**
    * Need it for testing!
    * @param id
    * @param dsPath
    * @param sModel
    * @param derivedFeatureFile
    * @return
    */
  def modelPrediction(id: ModelID,
                      dsPath: Path,
                      sModel: SerializableMLibClassifier,
                      derivedFeatureFile: Path): Try[PredictionObject] = Try {

    // loading data in the format suitable for data-integration project
    logger.info("   starting with csv reading for prediction...")
    val absFilePath = Paths.get(dsPath.getParent.toString, dsPath.getFileName.toString).toString
    val dataset = CsvDataLoader().load(absFilePath)
    logger.info("   csv file for prediction has been read!")

    val randomForestClassifier = MLibSemanticTypeClassifier(
      sModel.classes,
      sModel.model,
      sModel.featureExtractors,
      None,
      Option(derivedFeatureFile.toString),
      getBaggingParams(id))

    randomForestClassifier.predict(List(dataset))
  }

  /**
    * Performs prediction for a specified dataset using the model
    * and returns predictions for the specified dataset in the repository
    * @param id id of the model
    * @param dsPath path of the dataset
    * @param sModel Serialized Mlib classifier
    * @param dataSetID id of the dataset
    * @return DataSetPrediction wrapped in Option
    */
  def runPrediction(id: ModelID,
                    dsPath: Path,
                    sModel: SerializableMLibClassifier,
                    dataSetID: DataSetID): Option[DataSetPrediction] = {

    val derivedFeatureFile = predictionsPath(id, dataSetID)

    // TODO: Fix how this works, the writing and reading to files is unnecessary
    modelPrediction(id, dsPath, sModel, derivedFeatureFile) match {
      case Success(_) =>
        Try {
          readPredictions(derivedFeatureFile, sModel.classes.size, id, dataSetID)
        } toOption
      case Failure(err) =>
        logger.warn(s"Prediction for the dataset $dsPath failed: $err")
        None
    }
  }

  /**
    * The format for the data-integration line
    * @param id The name of the column in data-integration format
    * @param label The label given to the column
    * @param confidence The confidence the predictor has for the label
    * @param classes The values for the class confidences
    * @param features The feature vectors
    */
  case class CSVLine(id: String,
                     label: String,
                     confidence: String,
                     classes: List[String],
                     features: List[String])

  /**
    * For the body of the data-integration code, the confidence, class and
    * feature values are doubles. Here we simply convert them over
    * @param id The name of the column in data-integration format
    * @param label The label given to the column
    * @param confidence The confidence the predictor has for the label
    * @param classes The values for the class confidences
    * @param features The feature vectors
    */
  case class CSVDataLine(id: String,
                         label: String,
                         confidence: Double,
                         classes: List[Double],
                         features: List[Double])

  object CSVDataLine {
    def apply(line: CSVLine): CSVDataLine = {
      CSVDataLine(
        line.id,
        line.label,
        line.confidence.toDouble,
        line.classes.map(_.toDouble),
        line.features.map(_.toDouble))
    }
  }

  /**
    * Function to read a line in the data-integration format
    * @param list A line in the csv
    * @param classNum The number of classes selected
    * @return
    */
  protected def readLine(list: Seq[String], classNum: Int): CSVLine = {

    val line = list.iterator
    val id = line.next
    val label = line.next
    val confidence = line.next
    val classes = line.take(classNum).toList // extract the class names from the header
    val features = line.take(list.size).toList

    CSVLine(id, label, confidence, classes, features)
  }

  /**
    * Read predictions from the csv file
    * @param filePath string which indicates the location of the file with predictions
    * @param classNum number of classes in the model
    * @param modelID id of the model
    * @return List of ColumnPrediction
    */
  protected def readPredictions(filePath: Path, classNum : Int, modelID: ModelID, dsID: DataSetID): DataSetPrediction = {
    logger.info(s"Reading predictions from: $filePath...")

    //val reader = CSVReader.open(filePath.toFile).all
    // first load a CSV object...
    val reader = CSVFormat.RFC4180
      .parse(new FileReader(filePath.toFile))
      .iterator
      .asScala
      .map { row => (0 until row.size()).map(row.get) }
      .toList

    val header = readLine(reader.head, classNum)
    logger.info(s"    got header")
    val predictions = for {
      line <- reader.tail
      dataLine = CSVDataLine(readLine(line, classNum))
      scores = (header.classes zip dataLine.classes).toMap
      features = (header.features zip dataLine.features).toMap
    // this way colName is constructed if we use CSVHierarchicalDataLoader!!!
//      colName = dataLine.id.dropWhile(_ != '/').tail  // remove the filename at the start of the id...
      // remove the filename at the end -- if we use CSVDataLoader!!!
      colName = dataLine.id.split("@").dropRight(1).mkString("@") match {
        case "" => dataLine.id
        case c => c
      }
      column <- DatasetStorage.columnNameMap.get(dsID -> colName)
      prediction = column.id.toString -> ColumnPrediction(dataLine.label, dataLine.confidence, scores, features)
    } yield prediction

    logger.info(s"File read: $filePath...")
    DataSetPrediction(modelID, dsID, predictions.toMap)
  }

}
