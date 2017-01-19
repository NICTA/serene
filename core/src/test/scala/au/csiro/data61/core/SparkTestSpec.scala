package au.csiro.data61.core

import java.nio.file.Paths

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer}
import org.apache.spark.sql.SQLContext
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class SparkTestSpec extends FunSuite {

  val helperDir = getClass.getResource("/helper").getPath

  def setUpSpark(numWorkers: String ="1") : (SparkContext, SQLContext) = {
    //initialise spark stuff
    val conf = new SparkConf()
      .setAppName("SereneSchemaMatcher")
      .setMaster(s"local[$numWorkers]")
      .set("spark.driver.allowMultipleContexts", "true")
      //        .set("spark.rpc.netty.dispatcher.numThreads","2") //https://mail-archives.apache.org/mod_mbox/spark-user/201603.mbox/%3CCAAn_Wz1ik5YOYych92C85UNjKU28G+20s5y2AWgGrOBu-Uprdw@mail.gmail.com%3E
      .set("spark.network.timeout", "800s")
    //        .set("spark.executor.heartbeatInterval", "20s")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val sqlContext = new SQLContext(sc)
    (sc, sqlContext)
  }

  def trainRandomForest(sc: SparkContext, sqlContext: SQLContext)
  : (RandomForestClassificationModel, Double) = {
    // Load and parse the data file, converting it to a DataFrame.
    val corFile = Paths.get(helperDir, "sample_libsvm_data.txt").toString
    val data = sqlContext.read.format("libsvm").load(corFile)

    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(data)
    // Automatically identify categorical features, and index them.
    // Set maxCategories so features with > 4 distinct values are treated as continuous.
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(data)

    // Split the data into training and test sets (30% held out for testing)
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3), seed=1001)

    // Train a RandomForest model.
    val rf = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setNumTrees(10)
      .setMaxDepth(30)
      .setSeed(5043)

    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels)

    // Chain indexers and forest in a Pipeline
    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))

    // Train model.  This also runs the indexers.
    val model = pipeline.fit(trainingData)

    // Make predictions.
    val predictions = model.transform(testData)

    // Select example rows to display.
    predictions.select("predictedLabel", "label", "features").show(5)

    // Select (prediction, true label) and compute test error
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("precision")
    val accuracy = evaluator.evaluate(predictions)
    println("Test Error = " + (1.0 - accuracy))

    val rfModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
    println("Learned classification forest model:\n" + rfModel.toDebugString)

    (rfModel, accuracy)
  }

  test("one worker vs two workers") {
    val (sc, sqlContext) = setUpSpark("1")
    val (oneCoreModel, accu1) = trainRandomForest(sc, sqlContext)
    sc.stop()

    val (sc2, sqlContext2) = setUpSpark("4")
    val (twoCoreModel, accu2) = trainRandomForest(sc2, sqlContext2)
    sc2.stop()


    assert(accu1 === accu2)
    assert(oneCoreModel.numClasses === twoCoreModel.numClasses)
    assert(oneCoreModel.numFeatures === twoCoreModel.numFeatures)
    assert(oneCoreModel.treeWeights === twoCoreModel.treeWeights)
    assert(oneCoreModel.numTrees === twoCoreModel.numTrees)

    assert(oneCoreModel.totalNumNodes === twoCoreModel.totalNumNodes)
    assert(oneCoreModel.featureImportances === twoCoreModel.featureImportances)
  }
}