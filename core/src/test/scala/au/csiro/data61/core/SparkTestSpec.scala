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
package au.csiro.data61.core

import java.nio.file.Paths

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import scala.util.{Failure, Success, Try}

/**
  *  these are tests for spark directly
  */
@RunWith(classOf[JUnitRunner])
class SparkTestSpec extends FunSuite with LazyLogging{

  val helperDir = getClass.getResource("/helper").getPath

  def setUpSpark2(numWorkers: String ="1"): SparkSession = {
    val sparkSession = SparkSession.builder.
      master(s"local[$numWorkers]")
      .appName("spark session example")
      .getOrCreate()
    sparkSession
  }

  def readData(fileName: String)(implicit spark: SparkSession): (DataFrame, List[String]) = {
    // Load and parse the data file, converting it to a DataFrame.
    //    val corFile = Paths.get(helperDir, "sample_libsvm_data.txt").toString
    //    val data: DataFrame = spark.read.format("libsvm").load(corFile)
    val corFile = Paths.get(helperDir, fileName).toString

    val featurenames = fileName match {
      case "train_error2.csv" => (1 to 179).map(x => s"feature$x")
      case "train_error.csv" => (1 to 401).map(x => s"feature$x")
      case "small_train_error.csv" => (1 to 15).map(x => s"feature$x")
      case _ => throw new Exception("Can't handle that!")
    }

    // /construct schema
    val schema: StructType = StructType(
      StructField("class", StringType, false) +: featurenames
        .map { n =>
          StructField(n, DoubleType, true)
        }
    )
    val data: DataFrame = spark.read
      .format("csv")
      .schema(schema)
      .load(corFile)
//      .option("header", "true") //reading the headers
//      .option("mode", "DROPMALFORMED")
//      .option("inferSchema", "true")

    // debugging stuff
//    println(s" Schema: ${data.schema}")
//    println(s" FeatureNames ${data.schema.fieldNames.toList}")
//
//    println("***********")
//    data.show()
//    println("***********")

    (data, data.schema.fieldNames.toList.tail)
  }

  def trainRandomForest(data: DataFrame,
                        featurenames: List[String])(implicit spark: SparkSession)
  : (RandomForestClassificationModel, Double) = {
    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.
    val labelIndexer = new StringIndexer()
      .setInputCol("class")
      .setOutputCol("label")
      .fit(data)

    val featureIndexer: VectorAssembler = new VectorAssembler()
      .setInputCols(featurenames.toArray)
      .setOutputCol("features")

    // Split the data into training and test sets (30% held out for testing).
    val Array(trainingData, testData) = data.randomSplit(Array(0.8, 0.2), seed=1002)

    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    // Train a RandomForest model.
    val rf = if (trainingData.count < 20) {
      new RandomForestClassifier()
        .setLabelCol("label")
        .setFeaturesCol("features")
        .setNumTrees(10)
        .setMaxDepth(10)
        .setImpurity("gini")
        .setSeed(5043)
    } else {
      new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(128) // according to some works setting numTrees>128 is unneccessary
      .setMaxDepth(10)
      .setImpurity("gini")
      .setSeed(5043)
    }


    // Chain indexers and forest in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))

    // Train model. This also runs the indexers.
    val model = pipeline.fit(trainingData)

    // Make predictions.
    val predictions = model.transform(testData)

    // Select example rows to display.
//    predictions.show(5)

    // Select (prediction, true label) and compute test error.
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy: Double = evaluator.evaluate(predictions)
    println("Test Error = " + (1.0 - accuracy))

    val rfModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
//    println("Learned classification forest model:\n" + rfModel.toDebugString)

    (rfModel, accuracy)
  }

  test("Checking train_error2") {
    // issue with header features on columns with no headers
    implicit val spark = setUpSpark2("2")
    Try {
      val (data,featurenames) = readData("train_error2.csv")
      val (oneCoreModel, accu1) = trainRandomForest(data, featurenames)
    } match {
      case Success(_) =>
        spark.close()
        succeed
      case Failure(err) =>
        spark.close()
        fail(err)
    }
  }

  // FIXME: this bug hasn't been fixed yet! hoping that new release of spark will discard this issue
//  test("Checking train_error") {
//    // spark bug
//    // issue with too many features
//    implicit val spark = setUpSpark2("2")
//    val (data,featurenames) = readData("train_error.csv")
//    val (oneCoreModel, accu1) = trainRandomForest(data, featurenames)
//    spark.close()
//    succeed
//  }

  test("Business train error") {
    // issue that the training dataset is super small
    implicit val spark = setUpSpark2("2")
    Try {
      val (data, featurenames) = readData("small_train_error.csv")
      val (oneCoreModel, accu1) = trainRandomForest(data, featurenames)
    } match {
      case Success(_) =>
        spark.close()
        succeed
      case Failure(err) =>
        spark.close()
        fail(err)
    }

  }

}