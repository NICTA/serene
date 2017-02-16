///**
//  * Copyright (C) 2015-2016 Data61, Commonwealth Scientific and Industrial Research Organisation (CSIRO).
//  * See the LICENCE.txt file distributed with this work for additional
//  * information regarding copyright ownership.
//  *
//  * Licensed under the Apache License, Version 2.0 (the "License");
//  * you may not use this file except in compliance with the License.
//  * You may obtain a copy of the License at
//  *
//  * http://www.apache.org/licenses/LICENSE-2.0
//  *
//  * Unless required by applicable law or agreed to in writing, software
//  * distributed under the License is distributed on an "AS IS" BASIS,
//  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  * See the License for the specific language governing permissions and
//  * limitations under the License.
//  */
//package au.csiro.data61.core
//
//import java.nio.file.Paths
//
//import org.apache.spark
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.ml.Pipeline
//import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
//import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
//import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler, VectorIndexer}
//import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
//import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}
//import org.junit.runner.RunWith
//import org.scalatest.FunSuite
//import org.scalatest.junit.JUnitRunner
//
//
//@RunWith(classOf[JUnitRunner])
//class SparkTestSpec extends FunSuite {
//
//  val helperDir = getClass.getResource("/helper").getPath
//
//  def setUpSpark(numWorkers: String ="1") : (SparkContext, SQLContext) = {
//    //initialise spark stuff
//    val conf = new SparkConf()
//      .setAppName("SereneSchemaMatcher")
//      .setMaster(s"local[$numWorkers]")
//      .set("spark.driver.allowMultipleContexts", "true")
//      //        .set("spark.rpc.netty.dispatcher.numThreads","2") //https://mail-archives.apache.org/mod_mbox/spark-user/201603.mbox/%3CCAAn_Wz1ik5YOYych92C85UNjKU28G+20s5y2AWgGrOBu-Uprdw@mail.gmail.com%3E
//      .set("spark.network.timeout", "800s")
//    //        .set("spark.executor.heartbeatInterval", "20s")
//    val sc = new SparkContext(conf)
//    sc.setLogLevel("WARN")
//    val sqlContext = new SQLContext(sc)
//    (sc, sqlContext)
//  }
//
//  def setUpSpark2(numWorkers: String ="1") = {
//    val sparkSession = SparkSession.builder.
//      master(s"local[$numWorkers]")
//      .appName("spark session example")
//      .getOrCreate()
//    sparkSession
//  }
//
//  def trainRandomForest(spark: SparkSession): (RandomForestClassificationModel, Double) = {
//    // Load and parse the data file, converting it to a DataFrame.
////    val corFile = Paths.get(helperDir, "sample_libsvm_data.txt").toString
////    val data: DataFrame = spark.read.format("libsvm").load(corFile)
//    val corFile = Paths.get(helperDir, "test.csv").toString
//
//    val featurenames = (1 to 25).map(x => s"feature$x")
//    // /construct schema
//    val schema: StructType = StructType(
//      StructField("label", StringType, false) +: featurenames
//        .map { n =>
//          StructField(n, DoubleType, false)
//        }
//    )
//
//
//    val data: DataFrame = spark.read
//      .format("csv")
//      .schema(schema)
////      .option("header", "true") //reading the headers
////      .option("mode", "DROPMALFORMED")
//      .load(corFile)
//
//    print(data.schema)
//    print(data.schema.fieldNames)
//
//    println("***********")
//    data.show(10)
//    data.write.csv(s"/tmp/test/testspark_${System.nanoTime()}.csv")
//    println("***********")
//
//    // Index labels, adding metadata to the label column.
//    // Fit on whole dataset to include all labels in index.
//    val labelIndexer = new StringIndexer()
//      .setInputCol("label")
//      .setOutputCol("indexedLabel")
//      .fit(data)
//    // Automatically identify categorical features, and index them.
//    // Set maxCategories so features with > 4 distinct values are treated as continuous.
////    val featureIndexer = new VectorIndexer()
////      .setInputCol("features")
////      .setOutputCol("indexedFeatures")
////      .setMaxCategories(4)
////      .fit(data)
//
//    val featureIndexer: VectorAssembler = new VectorAssembler()
//      .setInputCols(featurenames.toArray)
//      .setOutputCol("indexedFeatures")
//
//    // Split the data into training and test sets (30% held out for testing).
//    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3), seed=1002)
//
//    // Train a RandomForest model.
//    val rf = new RandomForestClassifier()
//      .setLabelCol("indexedLabel")
//      .setFeaturesCol("indexedFeatures")
//      .setNumTrees(10)
//      .setMaxDepth(30)
//      .setImpurity("gini")
//      .setSeed(5043)
//
//    // Convert indexed labels back to original labels.
//    val labelConverter = new IndexToString()
//      .setInputCol("prediction")
//      .setOutputCol("predictedLabel")
//      .setLabels(labelIndexer.labels)
//
//    // Chain indexers and forest in a Pipeline.
//    val pipeline = new Pipeline()
//      .setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))
//
//    // Train model. This also runs the indexers.
//    val model = pipeline.fit(trainingData)
//
//    // Make predictions.
//    val predictions = model.transform(testData)
//
//    // Select example rows to display.
////    predictions.select("predictedLabel", "label", "features").show(5)
//    predictions.show(5)
//
//    // Select (prediction, true label) and compute test error.
//    val evaluator = new MulticlassClassificationEvaluator()
//      .setLabelCol("indexedLabel")
//      .setPredictionCol("prediction")
//      .setMetricName("accuracy")
//    val accuracy: Double = evaluator.evaluate(predictions)
//    println("Test Error = " + (1.0 - accuracy))
//
//    val rfModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
//    println("Learned classification forest model:\n" + rfModel.toDebugString)
//
//    (rfModel, accuracy)
//  }
//
//  test("one worker vs four workers should learn the same model") {
//    val spark = setUpSpark2("2")
//    val (oneCoreModel, accu1) = trainRandomForest(spark)
//    spark.close()
//
//
//    val spark2 = setUpSpark2("4")
//    val (twoCoreModel, accu2) = trainRandomForest(spark2)
//    spark2.stop()
//
//    assert(accu1 === accu2)
//    assert(oneCoreModel.numClasses === twoCoreModel.numClasses)
//    assert(oneCoreModel.numFeatures === twoCoreModel.numFeatures)
//    assert(oneCoreModel.treeWeights === twoCoreModel.treeWeights)
//    assert(oneCoreModel.totalNumNodes === twoCoreModel.totalNumNodes)
//    assert(oneCoreModel.featureImportances === twoCoreModel.featureImportances)
//
////    assert(oneCoreModel.numTrees === twoCoreModel.numTrees)
//
//
//  }
//}