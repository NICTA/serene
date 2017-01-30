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
package au.csiro.data61.matcher.matcher.train

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.ml.classification._
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature._
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.types._
import au.csiro.data61.matcher.data._
import au.csiro.data61.matcher.matcher.MLibSemanticTypeClassifier
import au.csiro.data61.matcher.matcher.features._
import com.typesafe.scalalogging.LazyLogging

import scala.util.{Failure, Success, Try}

case class TrainMlibSemanticTypeClassifier(classes: List[String],
                                           doCrossValidation: Boolean = false
                                          ) extends TrainSemanticTypeClassifier with LazyLogging {
  val defaultDepth = 30
  val defaultNumTrees = 20
  val defaultImpurity = "gini"

  /**
    * If parallelFeatureExtraction is true, then use spark to preprocess attributes.
    * Otherwise, spark will not be used
    * preprocess attributes of the data sources - logical datatypes are inferred during this process
    * @param resampledAttrs List of resampled attributes
    * @param parallelFeatureExtraction boolean
    * @param spark Implicit Spark session object
    * @return
    */
  def preprocessAttributes(resampledAttrs: List[Attribute],
                           parallelFeatureExtraction: Boolean = true)(implicit spark: SparkSession)
  : List[PreprocessedAttribute] = {
    val preprocessor = DataPreprocessor()
    parallelFeatureExtraction match {
      case true =>
        val rdd = spark.sparkContext.parallelize(resampledAttrs)
        logger.info(s"Parallel preprocessing on ${rdd.partitions.length} partitions")
        Try {
          rdd
            .map(preprocessor.preprocess)
            .collect.toList
        } match {
          case Success(preprocessedTrainInstances) =>
            preprocessedTrainInstances
          case Failure(err) =>
            logger.error(s"Parallel preprocessing failed ${err.getMessage}")
            spark.stop()
            throw new Exception(s"Parallel preprocessing failed ${err.getMessage}")
        }
      case false =>
        resampledAttrs.map(preprocessor.preprocess)
    }
  }

  /**
    * By performing cross validation identify best numTrees, depth and impurity.
    * @param indexer
    * @param vectorAssembler
    * @param labelConverter
    * @param datadDf
    * @return
    */
  def performCrossValidation(indexer: StringIndexerModel,
                             vectorAssembler: VectorAssembler,
                             labelConverter: IndexToString,
                             datadDf: DataFrame
                            ): (Int, Int, String) = {
    val dt = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxDepth(30)
      .setNumTrees(20)
    val pipeline = new Pipeline()
      .setStages(Array(indexer, vectorAssembler, dt, labelConverter))

    //setup crossvalidation
    val paramGrid = new ParamGridBuilder()
      .addGrid(dt.maxDepth, Array(1, 5, 10, 20, 30))
      .addGrid(dt.numTrees, Array(1, 5, 10, 15, 20))
      .addGrid(dt.impurity, Array("entropy", "gini"))
      .build()
    val cv = new CrossValidator()
      .setNumFolds(10)
      .setEstimator(pipeline)
      .setEstimatorParamMaps(paramGrid)
      .setEvaluator(new MulticlassClassificationEvaluator)
      .setSeed(10857171) // needs to be added in version 2.0.0 mllib
    // for versions < 2.0.0 seed is fixed to 0

    logger.info("***Running crossvalidation ...")
    val model = cv.fit(datadDf)
    val bestModelPipeline = model
      .bestModel
      .asInstanceOf[PipelineModel]
    val bestModel = bestModelPipeline
      .stages(2)
      .asInstanceOf[RandomForestClassificationModel]
    val bestModelEstimator = bestModel
      .parent
      .asInstanceOf[RandomForestClassifier]

    // Find best parameters as determined by cross-validation
    // Use this to print out the whole model: println(bestModel.toDebugString)
    val bestDepth = bestModelEstimator.getMaxDepth
    val bestNumTrees = bestModelEstimator.getNumTrees
    val bestImpurity = bestModelEstimator.getImpurity
    logger.info("~~~~~~~~~~~ Crossvalidation Results ~~~~~~~~~~~")
    logger.info("    best depth:    " + bestDepth)
    logger.info("    best # trees:  " + bestNumTrees)
    logger.info("    best impurity: " + bestImpurity)

    (bestDepth, bestNumTrees, bestImpurity)
  }

  def getPipelineModel(data: List[Row],
                       schema: StructType,
                       featureNames: List[String]
                      )(implicit spark: SparkSession): PipelineModel = {
    Try {
      // FIXME: we have the problem here!
      // instead of creating RDD first and then dataframe, we should directly create dataframe
      // fixing numSlices we solve the issue of models being different depending on numWorkers
      logger.info("***Pipeline model...")
      val dataRdd = spark.sparkContext.makeRDD(data, numSlices = 1)
      val dataDf = spark.createDataFrame(dataRdd, schema)

      // for debugging puposes - verify that features are the same
//      println("***********")
//      dataDf.show(10)
//      dataDf.write.csv(s"/tmp/test/model${System.nanoTime()}.csv")
//      println("***********")

      //train random forest
      logger.info("***Training random forest")
      val indexer: StringIndexerModel = new StringIndexer()
        .setInputCol("class")
        .setOutputCol("label")
        .fit(dataDf)
      val vecAssembler: VectorAssembler = new VectorAssembler()
        .setInputCols(featureNames.toArray)
        .setOutputCol("features")

      val labelConverter: IndexToString = new IndexToString()
        .setInputCol("prediction")
        .setOutputCol("predictedLabel")
        .setLabels(indexer.labels)

      val (depth, ntrees, impurity) = if (doCrossValidation) {
        performCrossValidation(indexer, vecAssembler, labelConverter, dataDf)
      } else {
        (defaultDepth, defaultNumTrees, defaultImpurity)
      }

      val finalModelEstimator = new RandomForestClassifier()
        .setLabelCol("label")
        .setFeaturesCol("features")
        .setMaxDepth(depth)
        .setNumTrees(ntrees)
        .setImpurity(impurity)
        .setSeed(5043)

      val finalPipeline = new Pipeline()
        .setStages(Array(indexer, vecAssembler, finalModelEstimator, labelConverter))
      finalPipeline.fit(dataDf)
    } match {
      case Success(model) =>
        model
      case Failure(err) =>
        logger.error(s"Spark model training failed ${err.getMessage}")
        spark.stop()
        throw new Exception(s"Spark model training failed ${err.getMessage}")
    }
  }

  def setUpSpark(numWorkers: Option[Int] = None): SparkSession = {
    val ms: String = numWorkers match {
      case Some(0) => s"local" // should it rather be [*]?
      case Some(num: Int) => s"local[$num]"
      case _ => "local"
    }
    val sparkSession = SparkSession.builder
      .master(ms)
      .appName("SereneSchemaMatcher")
      .getOrCreate()

//    val sc = new SparkConf()
//    sparkSession.conf.set( "spark.serializer", "org.apache.spark.serializer.KryoSerializer" )
//    sc.registerKryoClasses(Array(classOf[Attribute],
//      classOf[DataModel],
//      classOf[PreprocessedAttribute]))
//    sparkSession.conf.set("spark.driver.allowMultipleContexts", "true")
//    sparkSession.conf.set("spark.rpc.netty.dispatcher.numThreads","2") //https://mail-archives.apache.org/mod_mbox/spark-user/201603.mbox/%3CCAAn_Wz1ik5YOYych92C85UNjKU28G+20s5y2AWgGrOBu-Uprdw@mail.gmail.com%3E
//    sparkSession.conf.set("spark.network.timeout", "800s")
//    sparkSession.conf.set("spark.executor.heartbeatInterval", "20s")
    sparkSession
  }

  /**
    * Extract all features for training and get feature names.
    * Depending on parallelFeatureExtraction, it's done using spark or without.
    * @param preprocessedAttributes
    * @param labels
    * @param featureExtractors
    * @param parallelFeatureExtraction
    * @param spark
    * @return
    */
  def extractModelFeatures(preprocessedAttributes: List[PreprocessedAttribute],
                           labels: SemanticTypeLabels,
                           featureExtractors: List[FeatureExtractor],
                           parallelFeatureExtraction: Boolean
                          )(implicit spark: SparkSession)
  :(List[(PreprocessedAttribute, List[Double], String)], List[String]) = {

    parallelFeatureExtraction match {
      case true =>
        FeatureExtractorUtil
          .extractTrainFeatures(preprocessedAttributes, labels, featureExtractors)
      case false =>
        //get feature names
        val featureNames: List[String] = featureExtractors.flatMap {
          case x: SingleFeatureExtractor => List(x.getFeatureName())
          case x: GroupFeatureExtractor => x.getFeatureNames()
        }
        // extract features
        val features: List[(PreprocessedAttribute, List[Double], String)] =
          FeatureExtractorUtil.extractFeatures(preprocessedAttributes, labels, featureExtractors)
        (features,featureNames)
    }

  }

  def resampleModelAttributes(trainingData: DataModel,
                              labels: SemanticTypeLabels,
                              trainingSettings: TrainingSettings)(implicit spark: SparkSession)
  : List[Attribute] ={
    val allAttributes = DataModel.getAllAttributes(trainingData)
    logger.info(s"   obtained ${allAttributes.size} attributes")
    //resampling
    val numBags = trainingSettings.numBags.getOrElse(5)
    val bagSize = trainingSettings.bagSize.getOrElse(50)
    val resampledAttrs = ClassImbalanceResampler // here seeds are fixed so output will be the same on the same input
      .resample(trainingSettings.resamplingStrategy, allAttributes, labels, bagSize, numBags)
    logger.info(s"   resampled ${resampledAttrs.size} attributes")
    resampledAttrs
  }

  override def train(trainingData: DataModel,
                     labels: SemanticTypeLabels,
                     trainingSettings: TrainingSettings,
                     postProcessingConfig: Option[Map[String,Any]],
                     numWorkers: Option[Int] = None,
                     parallelFeatureExtraction: Boolean = true
                    ): MLibSemanticTypeClassifier = {
    logger.info(s"***Training initialization for classes: $classes...")

    //initialise spark stuff
    implicit val spark = setUpSpark(numWorkers)

    //resampling
    val resampledAttrs = resampleModelAttributes(trainingData, labels, trainingSettings)

    // preprocess attributes of the data sources - logical datatypes are inferred during this process
    val preprocessedTrainInstances = preprocessAttributes(resampledAttrs, parallelFeatureExtraction)
    // generate feature extractors --> main thing here is construction of example-based feature extractors
    val featureExtractors = FeatureExtractorUtil
      .generateFeatureExtractors(classes, preprocessedTrainInstances, trainingSettings, labels)

    val (features: List[(PreprocessedAttribute, List[Double], String)], featureNames: List[String]) =
      extractModelFeatures(preprocessedTrainInstances, labels, featureExtractors, parallelFeatureExtraction)

    //construct schema
    val schema: StructType = StructType(
      StructField("class", StringType, false) +: featureNames
        .map { n =>
          StructField(n, DoubleType, false)
        }
    )

    //convert instance features into Spark Row instances
    logger.info(s"   extracted ${features.size} features")
    val data: List[Row] = features
      .map { case (p, fvals, label) =>
        Row.fromSeq(label +: fvals)
      }

    val finalModel = getPipelineModel(data, schema, featureNames)

    spark.stop()
    logger.info("***Training finished.")
    MLibSemanticTypeClassifier(
      classes,
      finalModel,
      featureExtractors,
      postProcessingConfig
    )
  }

}