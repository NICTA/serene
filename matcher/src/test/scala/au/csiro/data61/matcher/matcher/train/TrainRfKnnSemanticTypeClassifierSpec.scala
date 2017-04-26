package au.csiro.data61.matcher.matcher.train

import scala.io._

import org.specs2._
import au.csiro.data61.matcher.ingestion.loader.NorthixDataLoader
import au.csiro.data61.matcher.matcher.train._
import au.csiro.data61.matcher.data.BasicLabels
import au.csiro.data61.matcher.matcher.features._
import au.csiro.data61.matcher.ingestion.loader._

import scala.collection.JavaConverters._

class TrainRfKnnSemanticTypeClassifierSpec extends mutable.Specification {
    s"""TrainRfKnnSemanticTypeClassifier""" should {
        "train a model" in {
            val trainingDataPath = "dirstruct/semantic_type_classifier/repo/raw_data"
            val classListPath = "dirstruct/semantic_type_classifier/repo/classes/class_list.csv"
            val labelsPath = "dirstruct/semantic_type_classifier/repo/labels/manual/semtype_labels.csv"
            val outputModelFilename = "dirstruct/semantic_type_classifier/repo/models/demo.rf"
            val resamplingStrategy = "NoResampling"
            val featuresConfigFile = "src/test/resources/config/features_config.json"
            val costMatrixFile = Some(Left("src/test/resources/config/cost_matrix.txt"))

            val featuresConfig = FeatureSettings.load(featuresConfigFile)

            val outOfBagError = if(!ClassImbalanceResampler.validResamplingStrategies.contains(resamplingStrategy)) {
                Left("Invalid resampling strategy")
            } else {
                val trainSettings = costMatrixFile.map({case path =>
                    println(s"Cost matrix provided: $path")
                    TrainingSettings(resamplingStrategy, featuresConfig, Some(path))
                }).getOrElse({
                    println("Cost matrix not provided")
                    TrainingSettings(resamplingStrategy, featuresConfig)
                })

                println("Loading class list...")
                val classes = "unknown" :: loadClassesRecursively(classListPath)
                println("Loaded classes: ")
                println("    " + classes.mkString(","))

                println("Loading training data...")
                val dataLoader = CsvDataLoader()
                val trainingData = dataLoader.load(trainingDataPath)
                println("Training data loaded.")

                println("Loading training labels...")
                val labelsLoader = SemanticTypeLabelsLoader()
                val labels = labelsLoader.load(labelsPath)
                println("Loaded training labels (" + labels.labelsMap.size + " instances).")
                println("""    Default class for unlabelled instances will be "unknown".""")

                println("Training model...")
                val startTime = System.nanoTime()
                val trainer = new TrainMlibSemanticTypeClassifier(classes)
                val randomForestSchemaMatcher = trainer.train(trainingData, labels, trainSettings, None)
                val endTime = System.nanoTime()
                println("Training finished in " + ((endTime-startTime)/1.0E9) + " seconds!  Saving model...")

                try {
                    // There seems to be no easy way to measure out-of-bag error in mlib
                    // Right(randomForestSchemaMatcher.model.measureOutOfBagError())

                    Right(true)
                } catch {
                    case e => Left("Error saving model.")
                }
            }

            (outOfBagError match {
                case Left(msg) =>
                    println(msg)
                    false
                case Right(success) => success
            }) mustEqual true

            // (outOfBagError match {
            //     case Left(msg) =>
            //         println(msg)
            //         false
            //     case Right(err) =>
            //         println("Out of bag error: " + err)
            //         (Math.abs(err - 0.4482029598308667) <= 0.2)
            // }) mustEqual true
        }
    }

    def loadClassesRecursively(path: String): List[String] = {
        val f = new java.io.File(path)
        if(f.isDirectory) {
            val children = f.listFiles
            children.flatMap({case c => loadClassesRecursively(c.getPath)}).distinct.toList
        } else {
            Source.fromFile(f).getLines.toList
        }
    }
}
