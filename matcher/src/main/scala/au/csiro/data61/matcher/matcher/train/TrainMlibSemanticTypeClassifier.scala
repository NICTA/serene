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

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.ml.classification._
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import au.csiro.data61.matcher.data._
import au.csiro.data61.matcher.data.{Metadata => DintMeta}
import au.csiro.data61.matcher.matcher._
import au.csiro.data61.matcher.matcher.features._
import com.typesafe.scalalogging.LazyLogging

case class TrainMlibSemanticTypeClassifier(classes: List[String],
                                           doCrossValidation: Boolean = false
                                          ) extends TrainSemanticTypeClassifier with LazyLogging {
  val defaultDepth = 30
  val defaultNumTrees = 20
  val defaultImpurity = "gini"

  override def train(trainingData: DataModel,
                     labels: SemanticTypeLabels,
                     trainingSettings: TrainingSettings,
                     postProcessingConfig: scala.Option[Map[String,Any]]
                    ): MLibSemanticTypeClassifier = {
    logger.info(s"***Training initialization for classes: $classes...")

    //initialise spark stuff
    val conf = new SparkConf()
      .setAppName("SereneSchemaMatcher")
      .setMaster("local")
      .set("spark.driver.allowMultipleContexts", "true")
    // changing to Kryo serialization!!!
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.registerKryoClasses(Array(classOf[List[PreprocessedAttribute]],
      classOf[PreprocessedAttribute],
      classOf[DataModel],
      classOf[Attribute],
      classOf[FeatureExtractor],
      classOf[SemanticTypeLabels],
      classOf[List[FeatureExtractor]],
      classOf[DintMeta]))
//    conf.set("spark.kryo.registrationRequired", "true")
    implicit val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val allAttributes = DataModel.getAllAttributes(trainingData)
    logger.info(s"   obtained ${allAttributes.size} attributes")
    //resampling
    val numBags = trainingSettings.numBags.getOrElse(100)
    val bagSize = trainingSettings.bagSize.getOrElse(100)
    val resampledAttrs = ClassImbalanceResampler // here seeds are fixed so output will be the same on the same input
      .resample(trainingSettings.resamplingStrategy, allAttributes, labels, bagSize, numBags)
    logger.info(s"   resampled ${resampledAttrs.size} attributes")

    // preprocess attributes of the data sources - logical datatypes are inferred during this process
    val preprocessor = DataPreprocessor()
    val preprocessedTrainInstances: List[PreprocessedAttribute] = resampledAttrs
      .map(rawAttr => preprocessor.preprocess(rawAttr))
    val featureExtractors = FeatureExtractorUtil
      .generateFeatureExtractors(classes, preprocessedTrainInstances, trainingSettings, labels)

    //get feature names and construct schema
    val featureNames = featureExtractors.flatMap({
        case x: SingleFeatureExtractor => List(x.getFeatureName())
        case x: GroupFeatureExtractor => x.getFeatureNames()
    })
    val schema = StructType(
        StructField("class", StringType, false) +: featureNames
          .map { case n =>
            StructField(n, DoubleType, false)
          }
    )

    //convert instance features into Spark Row instances
    val features = FeatureExtractorUtil
      .extractFeatures(preprocessedTrainInstances, labels, featureExtractors)(sc)

    logger.info(s"   extracted ${features.size} features")
    val data = features
      .map { case (p, fvals, label) =>
        Row.fromSeq(label +: fvals)
      }

    logger.info("***Spark parallelization")
    val dataRdd = sc.parallelize(data)
    val dataDf = sqlContext.createDataFrame(dataRdd, schema)

    //train random forest
    logger.info("***Training random forest")
    val indexer = new StringIndexer()
      .setInputCol("class")
      .setOutputCol("label")
      .fit(dataDf)
    val vecAssembler = new VectorAssembler()
      .setInputCols(featureNames.toArray)
      .setOutputCol("features")
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(indexer.labels)

    val (depth, ntrees, impurity) = if(doCrossValidation) {
        val dt = new RandomForestClassifier()
          .setLabelCol("label")
          .setFeaturesCol("features")
          .setMaxDepth(30)
          .setNumTrees(20)
        val pipeline = new Pipeline()
          .setStages(Array(indexer, vecAssembler, dt, labelConverter))

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
          //.setSeed((new Random(10857171))) // needs to be added in version 2.0.0 mllib
        // for versions < 2.0.0 seed is fixed to 0

        logger.info("***Running crossvalidation ...")
        val model = cv.fit(dataDf)
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
    } else {
        (defaultDepth, defaultNumTrees, defaultImpurity)
    }

    val finalModelEstimator = new RandomForestClassifier()
        .setLabelCol("label")
        .setFeaturesCol("features")
        .setMaxDepth(depth)
        .setNumTrees(ntrees)
        .setImpurity(impurity)

    val finalPipeline = new Pipeline()
      .setStages(Array(indexer, vecAssembler, finalModelEstimator, labelConverter))
    val finalModel = finalPipeline.fit(dataDf)
    sc.stop()

    logger.info("***Training finished.")
    MLibSemanticTypeClassifier(
        classes,
        finalModel,
        featureExtractors,
        postProcessingConfig
    )
  }

}
