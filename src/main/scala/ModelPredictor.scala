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

import au.csiro.data61.matcher.api.{BadRequestException, InternalException, NotFoundException}
import au.csiro.data61.matcher.storage.{DatasetStorage, ModelStorage}
import au.csiro.data61.matcher.types.DataSetTypes.DataSetID
import au.csiro.data61.matcher.types.ModelTypes.{Model, ModelID}
import com.nicta.dataint.matcher.MLibSemanticTypeClassifier
import com.nicta.dataint.matcher.train.TrainAliases.PredictionObject
import org.joda.time.DateTime
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FilenameUtils

import scala.util.{Failure, Success, Try}

// data integration project
import com.nicta.dataint.data.{DataModel, SemanticTypeLabels}
import com.nicta.dataint.matcher.train.{TrainMlibSemanticTypeClassifier, TrainingSettings}
import com.nicta.dataint.matcher.features.FeatureSettings
import com.nicta.dataint.ingestion.loader.{CSVHierarchicalDataLoader, SemanticTypeLabelsLoader}
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
  def predict(id: ModelID): List[Option[PredictionObject]] = {
    // read in the learnt model
    val serialMod = readLearntModelFile(ModelStorage.modelPath(id).toString)
      .getOrElse(throw InternalException(s"Failed reading learnt model for $id"))

    val preds = DatasetStorage
      .getCSVResources // get all csv files from data repository
      .map(predictDataset(id, _, serialMod)) // predict each dataset
    preds
  }

  /**
    * Performs prediction for the model and returns predictions for all datasets in the repository
    *
    * @param id id of the model
    * @param dsPath path of the dataset
    * @param sModel Serialized Mlib classifier
    * @return Serialized Mlib classifier wrapped in Option
    */
  def predictDataset(id: ModelID
                     , dsPath: String
                     , sModel: SerializableMLibClassifier): Option[PredictionObject] = {
    // TODO: check if prediction is already available
    val dataset = CSVHierarchicalDataLoader().readDataSet(dsPath,"")
    val dsName = s"${FilenameUtils.getBaseName(dsPath)}.${FilenameUtils.getExtension(dsPath)}"
    val derivedFeatureFile = Paths.get(ModelStorage.getPredictionsPath(id).toString,dsName).toString
    val randomForestClassifier = MLibSemanticTypeClassifier(
      sModel.classes, sModel.model, sModel.featureExtractors, None, Option(derivedFeatureFile))
    //Option(randomForestClassifier.predict(List(dataset)))
    Try(randomForestClassifier.predict(List(dataset))) match {
      case Success(preds) => Some(preds)
      case Failure(err) => {
        // right now we fail the whole prediction process
        logger.error(s"Prediction for dataset $dsPath failed: $err")
        None
        //throw InternalException(s"Prediction for dataset $dsPath failed: $err")
        // TODO: should we rather return just None for this dataset???
      }
    }
  }

}
