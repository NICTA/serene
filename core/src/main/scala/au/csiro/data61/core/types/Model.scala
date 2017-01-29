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
package au.csiro.data61.core.types

import java.nio.file.{Files, Path}

import au.csiro.data61.core.storage.{ModelStorage, DatasetStorage}
import au.csiro.data61.core.storage.ModelStorage._
import au.csiro.data61.core.types.DataSetTypes._
import au.csiro.data61.core.types.ModelTypes.Status.COMPLETE
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.ml.{PipelineModel, Pipeline}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.DateTime
import ModelTypes._

import scala.util.{Try, Failure, Success}

object Model {
  val spark = SparkSession.builder
    .appName("Serene")
    .config("spark.master", "local[*]")
    .getOrCreate()
}

/**
  * The Main model class
  *
  * @param description
  * @param id
  * @param modelType
  * @param classes
  * @param features
  * @param costMatrix
  * @param resamplingStrategy
  * @param labelData
  * @param refDataSets
  * @param modelPath
  * @param state
  * @param dateCreated
  * @param dateModified
  * @param numBags
  * @param bagSize
  */
case class Model(description: String,
                 id: ModelID,
                 modelType: ModelType,
                 classes: List[String],
                 features: FeaturesConfig,
                 costMatrix: List[List[Double]],
                 resamplingStrategy: SamplingStrategy,
                 labelData: Map[Int, String], // WARNING: Int should be ColumnID! Json4s bug.
                 refDataSets: List[Int],   // WARNING: Int should be DataSetID! Json4s bug.
                 modelPath: Option[Path],
                 state: TrainState,
                 dateCreated: DateTime,
                 dateModified: DateTime,
                 numBags: Option[Int],
                 bagSize: Option[Int]) extends Identifiable[ModelID] with LazyLogging {

  /**
    * predict column classes for dataset at datasetID
    *
    * @param datasetID
    * @return
    */
  def predict(datasetID: DataSetID): DataSetPrediction = {

    val output = for {
      ds <- DatasetStorage.get(datasetID)
      cols = ds.columns.map(_.id.toString)
    } yield cols.map(_ -> ColumnPrediction(
      classes.head,
      1.0,
      Map.empty[String, Double],
      Map.empty[String, Double])
    )

    DataSetPrediction(id, datasetID, output.get.toMap)
  }

  /**
    * storeModel
    *
    * @param learnedModel
    * @return
    */
  protected def storeModel(learnedModel: PipelineModel): Try[Path] = {
    Try { ModelStorage.addModel(id, learnedModel).get }
  }

  /**
    * Train the model over the given labelData
    *
    * @return
    */
  def train: Status = {

    ModelStorage.updateTrainState(id, Status.BUSY, "")

    (for {
        rawDF <- columnExtract()
        featureDF <- featureExtract(rawDF)
        resampledDF <- classResampling(featureDF)
        pipeline <- learner(resampledDF)
        path <- storeModel(pipeline)
      } yield path)

    match {

      case Success(path) =>
        ModelStorage.updateTrainState(id, Status.COMPLETE)
        Status.COMPLETE

      case Success(_) =>
        ModelStorage.updateTrainState(id, Status.ERROR)
        Status.ERROR

      case Failure(err) =>
        logger.error(err.getMessage)
        ModelStorage.updateTrainState(id, Status.ERROR)
        Status.ERROR
    }
  }

  protected type TrainingData = List[(String, Column[Any])]

  protected def columnExtract(): Try[TrainingData] = {
    Success(List())
  }

  protected def featureExtract(a: TrainingData]): Try[DataFrame] = {
    Success(Model.spark.createDataFrame())
  }

  protected def classResampling(df: DataFrame): Try[DataFrame] = {
    // Prepare training documents from a list of (id, text, label) tuples.
    val a = Model.spark.createDataFrame(Seq(
      (0L, "a b c d e spark", 1.0),
      (1L, "b d", 0.0),
      (2L, "spark f g h", 1.0),
      (3L, "hadoop mapreduce", 0.0)
    )).toDF("id", "text", "label")

    Success(a)
  }

  /**
    * The learner step...
    *
    * @return
    */
  protected def learner(df: DataFrame): Try[PipelineModel] = {

    val HashingLength = 1000

    Try {

      // single column
      //    - character distributions
      //    - type detection
      //    - entropy
      //
      // header-to-class comparisons
      //    - approx nearest neighbours
      //
      // values-to-class-values comparisons
      //    - approx nearest neighbours
      //    - tf?

      // Configure an ML pipeline, which consists of three stages: tokenizer, hashingTF, and lr.
      val tokenizer = new Tokenizer()
        .setInputCol("text")
        .setOutputCol("words")
      val hashingTF = new HashingTF()
        .setNumFeatures(HashingLength)
        .setInputCol(tokenizer.getOutputCol)
        .setOutputCol("features")
      val lr = new LogisticRegression()
        .setMaxIter(10)
        .setRegParam(0.01)
      val pipeline = new Pipeline()
        .setStages(Array(tokenizer, hashingTF, lr))

      // Fit the pipeline to training documents.
      val model = pipeline.fit(df)
      model
    }
  }

  /**
    * Check if the trained model is consistent.
    * This means that the model file is available, and that the datasets
    * have not been updated since the model was last modified.
    *
    * @return boolean
    */
  def isConsistent: Boolean = {
    logger.info(s"Checking consistency of model $id")

    // make sure the datasets in the model are older
    // than the training state
    val isOK = for {
      model <- get(id)
      path = model.modelPath
      trainDate = model.state.dateChanged
      refIDs = model.refDataSets
      refs = refIDs.flatMap(DatasetStorage.get).map(_.dateModified)

      // make sure the model is complete
      isComplete = model.state.status == COMPLETE

      // make sure the datasets are older than the training date
      allBefore = refs.forall(_.isBefore(trainDate))

      // make sure the model file is there...
      modelExists = path.exists(Files.exists(_))

    } yield {
      allBefore && modelExists && isComplete
    }

    isOK getOrElse false
  }

}