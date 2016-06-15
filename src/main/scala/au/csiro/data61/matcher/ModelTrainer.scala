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

import java.io.File
import java.nio.file.Paths

import au.csiro.data61.matcher.types.{Feature, ModelType, SamplingStrategy}
import au.csiro.data61.matcher.types.ModelTypes.{Model, ModelID}
import org.joda.time.DateTime

// data integration project
import com.nicta.dataint.data.{DataModel, SemanticTypeLabels}
import com.nicta.dataint.matcher.SemanticTypeClassifier
import com.nicta.dataint.matcher.features.FeatureSettings
import com.nicta.dataint.matcher.train.{CostMatrixConfig, TrainMlibSemanticTypeClassifier, TrainingSettings}
import com.nicta.dataint.matcher.features.FeatureSettings
import com.nicta.dataint.data.DataModel
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

object ModelTrainer {

  //  parsing step
  //  val labelsLoader = SemanticTypeLabelsLoader()
  //  val labels = labelsLoader.load(appConfig.labelsPath)
  //  val datasets = servicesConfig.dataSetRepository.getDataModels(appConfig.rawDataPath)
  //  val featuresConfig = FeatureSettings.load(appConfig.featuresConfigPath, appConfig.repoPath)

  // training step!
  //  val trainSettings = TrainingSettings(resamplingStrategy, featuresConfig, costMatrixConfigOption)
  //  val trainingData = new DataModel("", None, None, Some(datasets))
  //  val trainer = new TrainMlibSemanticTypeClassifier(classes, false)
  //  val randomForestSchemaMatcher = trainer.train(trainingData, labels, trainSettings, postProcessingConfig)

  val rootDir: String = ModelStorage.rootDir
  val datasetDir: String = DatasetStorage.rootDir

  /*
   Return an instance of class TrainingSettings
    */
  def readSettings(trainerPaths: ModelTrainerPaths): TrainingSettings = {
    println(s"featuresConfig: ${trainerPaths.featuresConfigPath}, workspacePath: ${trainerPaths.workspacePath}")
    val featuresConfig = FeatureSettings.load(trainerPaths.featuresConfigPath, trainerPaths.workspacePath)
//    val featuresConfig = FeatureSettings.load(trainerPaths.featuresConfigPath)
    println("Features have been loaded")
    TrainingSettings(trainerPaths.curModel.resamplingStrategy.str,
      featuresConfig,
      Some(Left(trainerPaths.costMatrixConfigPath)))
  }

  /*
   Returns a list of DataModel instances at path
    */
  def getDataModels(path: String): List[DataModel] = {
    val csvres = DatasetStorage.getCSVResources
    println("****")
    println(s"resources: $csvres")
    println("****")
      csvres.map{CSVHierarchicalDataLoader()
        .readDataSet(_,"")}
  }

  /*
   Returns a list of DataModel instances for the dataset repository
    */
  def readTrainingData(trainerPaths: ModelTrainerPaths): DataModel = {
    val datasets = getDataModels(datasetDir)
    val a = new DataModel("", None, None, Some(datasets))
    println(s"Datamodels created: ${datasets.length}")
    a
  }

  /*
   Reads in labeled data
    */
  def readLabeledData(trainerPaths: ModelTrainerPaths): SemanticTypeLabels ={
    val labelsLoader = SemanticTypeLabelsLoader()
    labelsLoader.load(trainerPaths.labelsDirPath)
  }

  /*
   Performs training for the model and returns serialized object for the learnt model
    */
  def train(id: ModelID): Option[SerializableMLibClassifier] = {
    ModelStorage.identifyPaths(id)
      .map(cts  => {
//        print("paths identified")
        DataintTrainModel(classes = cts.curModel.labels,
          trainingSet = readTrainingData(cts),
          labels = readLabeledData(cts),
          trainSettings = readSettings(cts),
          postProcessingConfig = None)})
      .map(dt => {
        // TODO: raise error if trainingSet is empty, i.e., no raw data
        val trainer = TrainMlibSemanticTypeClassifier (dt.classes, false)
        val randomForestSchemaMatcher = trainer.train(dt.trainingSet,
          dt.labels,
          dt.trainSettings,
          dt.postProcessingConfig)
        SerializableMLibClassifier(randomForestSchemaMatcher.model,
          dt.classes,
          randomForestSchemaMatcher.featureExtractors,
          randomForestSchemaMatcher.postProcessingConfig)
      })

  }

//  def train(resamplingStrategy: String,
//            featuresConfig: FeatureSettings,
//            costMatrixConfig: Option[CostMatrixConfig],
//            trainingSet: DataModel,
//            labels: SemanticTypeLabels,
//            classes: List[String],
//            postProcessingConfig: Option[Map[String,Any]]): SemanticTypeClassifier = {
//    val trainSettings = TrainingSettings(resamplingStrategy, featuresConfig, costMatrixConfig.map({case x => Right(x)}))
//    val startTime = System.nanoTime()
//    val trainer = TrainMlibSemanticTypeClassifier(classes, false)
//    val randomForestSchemaMatcher = trainer.train(trainingSet, labels, trainSettings, postProcessingConfig)
//    val endTime = System.nanoTime()
//    println("Training finished in " + ((endTime-startTime)/1.0E9) + " seconds.")
//
//    //allAttributes zip predsReordered -- attributes with predicted labels
//    randomForestSchemaMatcher
//  }

}
