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

import scala.util.{Random, Failure, Success, Try}

/**
  * NOTE: in case the training data is small we need to reduce numTrees otherwise Spark fails!!!
  * @param classes List of strings for class labels
  * @param defaultDepth Default depth of trees in the random forest classifier; defaults to 10
  * @param defaultNumTrees Default number of trees in the random forest classifier; defaults to 128
  * @param defaultImpurity Default impurity; defaults to "gini"
  * @param doCrossValidation Boolean which indicates whether cross validation will be performed
  */
case class TrainMlibSemanticTypeClassifier(classes: List[String],
                                           defaultDepth: Int = 10,
                                           defaultNumTrees: Int = 128, // according to some works setting numTrees>128 is unneccessary
                                           defaultImpurity: String = "gini",
                                           doCrossValidation: Boolean = false
                                          ) extends TrainSemanticTypeClassifier with LazyLogging {

  /**
    * If parallelFeatureExtraction is true, then use spark to preprocess attributes.
    * Otherwise, spark will not be used
    * preprocess attributes of the data sources - logical datatypes are inferred during this process
    * @param resampledAttrs List of resampled attributes
    * @param parallelFeatureExtraction boolean
    * @param spark Implicit Spark session object
    * @return
    */
  protected def preprocessAttributes(resampledAttrs: List[Attribute],
                           parallelFeatureExtraction: Boolean = true)(implicit spark: SparkSession)
  : List[PreprocessedAttribute] = {
    val preprocessor = DataPreprocessor()
    if (parallelFeatureExtraction) {
        logger.info(s"Parallel preprocessing for training...")
        Try {
//          val rdd = spark.sparkContext.parallelize(resampledAttrs)
          val attrBroadcast = spark.sparkContext.broadcast(resampledAttrs)
          spark.sparkContext.parallelize(resampledAttrs.indices)
            .map(idx => preprocessor.preprocess(attrBroadcast.value(idx)))
            .collect.toList
        } match {
          case Success(preprocessedTrainInstances) =>
            logger.info(s"Finished parallel preprocessing for training.")
            preprocessedTrainInstances
          case Failure(err) =>
            logger.error(s"Parallel preprocessing failed ${err.getMessage}")
            spark.stop()
            throw new Exception(s"Parallel preprocessing failed ${err.getMessage}")
        }
    } else {
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
  private def performCrossValidation(indexer: StringIndexerModel,
                             vectorAssembler: VectorAssembler,
                             labelConverter: IndexToString,
                             datadDf: DataFrame
                            ): (Int, Int, String) = {
    val dt = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxDepth(10)
      .setNumTrees(128)
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

  /**
    * Identify numTrees, depth and impurity for the random forest classifier.
    * We either perform cross validation or use default settings.
    * However, we take into account that for small datasets numTrees needs to be very small.
    * @param indexer
    * @param vectorAssembler
    * @param labelConverter
    * @param datadDf
    * @return
    */
  private def calculateRFParameters(indexer: StringIndexerModel,
                                    vectorAssembler: VectorAssembler,
                                    labelConverter: IndexToString,
                                    datadDf: DataFrame
                                   ): (Int, Int, String) = {
    if (doCrossValidation) {
      // better don't do cross-validation
      performCrossValidation(indexer, vectorAssembler, labelConverter, datadDf)
    } else {
      if (datadDf.count < 20) {
        // we need to reduce the number of trees in case the training dataset is too small
        // this is done to avoid Spark error "empty.maxBy"
        (defaultDepth, 10, defaultImpurity)
      } else {
        (defaultDepth, defaultNumTrees, defaultImpurity)
      }
    }
  }

  /**
    * Costruct Spark Pipeline Model and train it.
    * @param data Training data converted to Spark Rows.
    * @param schema Schema of the data.
    * @param featureNames List of feature names.
    * @param spark Implicit Spark Session.
    * @return
    */
  protected def getPipelineModel(data: List[Row],
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
//       for debugging puposes - verify that features are the same
//      println("***********")
//      dataDf.show(10)
//      dataDf.write.csv(s"/tmp/test/model${System.nanoTime()}.csv")
//      logger.info(s"Constructed schema of the dataframe: ${dataDf.schema}")
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

      val (depth, ntrees, impurity) = calculateRFParameters(indexer, vecAssembler, labelConverter, dataDf)

      val finalModelEstimator = new RandomForestClassifier()
        .setLabelCol("label")
        .setFeaturesCol("features")
        .setMaxDepth(depth)
        .setNumTrees(ntrees)
        .setImpurity(impurity)
        .setSeed(5043)

      val finalPipeline = new Pipeline()
        .setStages(Array(indexer, vecAssembler, finalModelEstimator, labelConverter))
      // FIXME: if we have more than 400 features, this will fail!
      if (featureNames.size > 399) {
        logger.warn("Spark cannot handle situations when there are too many features!")
      }
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

  /**
    * Initialize SparkSession for the training.
    * This is standalone local spark setting.
    * @param numWorkers Number of cores to be used for spark initialization.
    * @return
    */
  protected def setUpSpark(numWorkers: Option[Int] = None): SparkSession = {
    val ms: String = numWorkers match {
      case Some(0) => s"local" // should it rather be [*]?
      case Some(num: Int) => s"local[$num]"
      case _ => "local"
    }

    val sparkSession = SparkSession.builder
      .master(ms)
      .appName("SereneSchemaMatcher")
      .getOrCreate()
    sparkSession.conf.set("spark.executor.cores","8")

    sparkSession
  }


  /**
    * Depending on parallelFeatureExtraction, it's done using spark or without.
    * @param preprocessedAttributes list of resampled attributes
    * @param labels semantic labels
    * @param featureExtractors list of feature extractors already generated
    * @param parallelFeatureExtraction whether to use spark for feature extraction
    * @param spark implicit spark session
    * @return
    */
  protected def extractModelFeatures(preprocessedAttributes: List[Attribute],
                           labels: SemanticTypeLabels,
                           featureExtractors: List[FeatureExtractor],
                           parallelFeatureExtraction: Boolean
                          )(implicit spark: SparkSession)
  :List[(List[Double], String)] = {

    if(parallelFeatureExtraction) {
      FeatureExtractorUtil
        .extractSimpleTrainFeatures(preprocessedAttributes, labels, featureExtractors)
    } else {
      // old style feature extraction...
      FeatureExtractorUtil.extractFeatures(preprocessedAttributes, labels, featureExtractors)
    }
  }

  /**
    * Helper funciton to perform resampling of the original attributes.
    * @param allAttributes List of attributes from the provided data sources.
    * @param labels List of semantic labels specified for the project.
    * @param trainingSettings Parameters for the model to be trained.
    * @param spark Implicit SparkSession
    * @return
    */
  protected def resampleModelAttributes(allAttributes: List[Attribute],
                                        labels: SemanticTypeLabels,
                                        trainingSettings: TrainingSettings)(implicit spark: SparkSession)
  : List[Attribute] ={
    //resampling
    val numBags = trainingSettings.numBags.getOrElse(DefaultBagging.NumBags)
    val bagSize = trainingSettings.bagSize.getOrElse(DefaultBagging.BagSize)
    // here seeds are fixed so output will be the same on the same input
    // NOTE: results might be different for a set of datasets, because the order in which they are concatenated matters
    val resampledAttrs = ClassImbalanceResampler
      .resample(trainingSettings.resamplingStrategy, allAttributes, labels, bagSize, numBags)
    logger.info(s"   resampled ${resampledAttrs.size} attributes")
    resampledAttrs
  }

  /**
    * Extract all features for training and get feature names.
    * In case of bagging we do something special!
    * @param allAttributes
    * @param labels
    * @param featureExtractors
    * @param trainingSettings
    * @param parallelFeatureExtraction
    * @param spark
    * @return
    */
  protected def sampleExtractFeatures(allAttributes: List[Attribute],
                                      labels: SemanticTypeLabels,
                                      featureExtractors: List[FeatureExtractor],
                                      trainingSettings: TrainingSettings,
                                      parallelFeatureExtraction: Boolean = true)(implicit spark: SparkSession):
  (List[(List[Double], String)], List[String])  = {

    val features: List[(List[Double], String)] = trainingSettings.resamplingStrategy match {
      case "Bagging" =>
        val numBags = trainingSettings.numBags.getOrElse(DefaultBagging.NumBags)
        val bagSize = trainingSettings.bagSize.getOrElse(DefaultBagging.BagSize)
        FeatureExtractorUtil.extractBaggingFeatures(allAttributes, labels,
          featureExtractors, numBags, bagSize)
      case _ =>
        val resampledAttrs = resampleModelAttributes(allAttributes, labels, trainingSettings)
        extractModelFeatures(resampledAttrs, labels, featureExtractors, parallelFeatureExtraction)
    }

    //get feature names
    val featureNames: List[String] = featureExtractors.flatMap {
      case x: SingleFeatureExtractor => List(x.getFeatureName())
      case x: GroupFeatureExtractor => x.getFeatureNames()
    }

    (features, featureNames)
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

    val allAttributes = DataModel.getAllAttributes(trainingData)

    logger.info(s"   obtained ${allAttributes.size} attributes")
    // generate feature extractors --> main thing here is construction of example-based feature extractors
    // we do it before resampling!
    val featureExtractors = FeatureExtractorUtil
      .generateSimpleFeatureExtractors(classes, allAttributes, trainingSettings, labels)

    val (features: List[(List[Double], String)], featureNames: List[String]) =
      sampleExtractFeatures(allAttributes, labels, featureExtractors, trainingSettings, parallelFeatureExtraction)

    //construct schema
    val schema: StructType = StructType(
      StructField("class", StringType, true) +: featureNames
        .map { n =>
          StructField(n, DoubleType, true) // nullable property
        }
    )
    //convert instance features into Spark Row instances
    logger.info(s"   extracted ${features.size} instances")
    logger.info(s"   feature vector size: ${featureNames.size}")
    val data: List[Row] = features.map {
        case (fvals, label) =>
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
