package au.csiro.data61.matcher.matcher.runner

import au.csiro.data61.matcher.data._
import au.csiro.data61.matcher.matcher._
import au.csiro.data61.matcher.matcher.eval.datasetutils._
import au.csiro.data61.matcher.matcher.eval.metrics._
import au.csiro.data61.matcher.ingestion.loader._
import au.csiro.data61.matcher.nlptools.distance.LCSubsequenceDistanceMetric

import au.csiro.data61.matcher.matcher.train._
import au.csiro.data61.matcher.matcher.serializable._
import au.csiro.data61.matcher.matcher.features._

import scala.collection.JavaConverters._
import scala.io._

import java.io._

/**
 *  This class is called by the dirstruct/semantic_type_classifier/train_semtype_classifier.sh script.  
 *  For more info on the training process, please see the TRAINING section in
 *  dirstruct/semantic_type_classifier/HOWTO.
 **/
object RunRfKnnSemanticTypeClassifierTraining {
    val usageMessage = """Usage:
                         #    java au.csiro.data61.matcher.matcher.runner.RunRfKnnSematicTypeClassifierTraining <raw_data_path> <class_list_path> <labels_path> <output_model_path> <resampling_strategy> <features_config> [cost_matrix_file]
                       """.stripMargin('#')

    def main(args: Array[String]) = {
        if(args.size < 6) {
            printUsage()
        } else {
            val trainingDataPath = args(0)
            val classListPath = args(1)
            val labelsPath = args(2)
            val outputModelFilename = args(3)
            val resamplingStrategy = args(4)
            val featuresConfigFile = args(5)
            val costMatrixFile = if(args.size >= 7) Some(args(6)) else None

            val featuresConfig = FeatureSettings.load(featuresConfigFile)

            if(!ClassImbalanceResampler.validResamplingStrategies.contains(resamplingStrategy)) {
                println("Invalid resampling strategy: " + resamplingStrategy)
                println("    Choose from: " + ClassImbalanceResampler.validResamplingStrategies.mkString(", "))
            } else {
                val trainSettings = costMatrixFile.map({case path =>
                    println(s"Cost matrix provided: $path")
                    TrainingSettings(resamplingStrategy, featuresConfig, Some(Left(path)))
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
                    val out = new ObjectOutputStream(new FileOutputStream(outputModelFilename))
                    out.writeObject(SerializableMLibClassifier(randomForestSchemaMatcher.model, classes, randomForestSchemaMatcher.featureExtractors))
                    out.close()
                    println(s"Model saved in $outputModelFilename")
                    println("ALL DONE!")
                } catch {
                    case e: java.io.IOException => println("Error saving model.")
                            e.printStackTrace
                }
            }
        }
    }

    def loadClassesRecursively(path: String): List[String] = {
        val f = new File(path)
        if(f.isDirectory) {
            val children = f.listFiles
            children.flatMap({case c => loadClassesRecursively(c.getPath)}).distinct.toList
        } else {
            Source.fromFile(f).getLines.toList
        }
    }

    def printUsage() = {
        println(usageMessage)
    }
}
