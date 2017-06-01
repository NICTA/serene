//package au.csiro.data61.matcher.matcher.runner
//
//import au.csiro.data61.matcher.data._
//import au.csiro.data61.matcher.matcher._
//import au.csiro.data61.matcher.matcher.eval.datasetutils._
//import au.csiro.data61.matcher.matcher.eval.metrics._
//import au.csiro.data61.matcher.ingestion.loader._
//import au.csiro.data61.matcher.nlptools.distance.LCSubsequenceDistanceMetric
//import au.csiro.data61.matcher.matcher.serializable._
//import au.csiro.data61.matcher.matcher.train._
//import java.io._
//import java.nio.file.Paths
//
///**
// *  This class is called by the dirstruct/semantic_type_classifier/predict_semtypes.sh script.
// *  For more info on the prediction process, please see the INFERENCE section in
// *  dirstruct/semantic_type_classifier/HOWTO.
// **/
//object RunRfKnnSemanticTypeClassifier {
//    val usageMessage = """Usage:
//                         #    java -cp prototype.jar au.csiro.data61.matcher.matcher.runner.RunRfKnnSemanticTypeClassifier <path-to-model> <path-to-dataset> <path-to-output>
//                       """.stripMargin('#')
//
//    def main(args: Array[String]) = {
//        if(args.size != 3) {
//            printUsage()
//        } else {
//            val modelPath = args(0)
//            val datasetPath = args(1)
//            val outputPath = args(2)
//
//            println("Loading model...")
//            val serializedModel: Either[String, SerializableMLibClassifier] = try {
//                val in = new ObjectInputStream(new FileInputStream(modelPath))
//                val data = in.readObject().asInstanceOf[SerializableMLibClassifier]
//                in.close()
//                println("Finished loading model.  Model has been trained with the ff. classes:")
//                println("    " + data.classes)
//                Right(data)
//            } catch {
//                case e: java.lang.Exception => {
//                    Left("Error reading model: " + e.getMessage)
//                }
//            }
//
//            serializedModel match {
//                case Right(data: SerializableMLibClassifier) =>
//                    val model = data.model
//                    val classes = data.classes
//                    val trainer = new TrainMlibSemanticTypeClassifier(classes)
//                    val derivedFeaturesFile = s"$outputPath.derivedfeatures.csv"
//                    val randomForestClassifier = MLibSemanticTypeClassifier(classes, model
//                        , data.featureExtractors, derivedFeaturesPath=Some(derivedFeaturesFile))
//
//                    println("Running model on datasets...")
//                    val dataset = CsvDataLoader().load(datasetPath)
//                    val startTime = System.nanoTime()
//                    val predictions = randomForestClassifier.predict(List(dataset))
//                    val endTime = System.nanoTime()
//                    println("Predictions computed in " + ((endTime-startTime)/1.0E9) + " seconds.")
//
//                    //get the class with the max
//                    val maxClassPreds = predictions.map({case (attr, scores, attrFeatures) =>
//                        val maxIdx = scores.zipWithIndex.maxBy({_._1})._2
//                        val maxScore = scores(maxIdx)
//                        val classPred = classes(maxIdx)
//                        (attr.id, classPred, maxScore)
//                    })
//
//                    //let's timestamp these predictions for measuring perf over time
//                    val dateFormat = new java.text.SimpleDateFormat("yyyy/MM/dd")
//                    val now = dateFormat.format(java.util.Calendar.getInstance.getTime)
//
//                    val out = new PrintWriter(new File(outputPath))
//                    out.println("attribute_id,predicted_class,confidence,date_predicted,actual_class,date_validated")
//                    out.println(maxClassPreds.sortBy(-_._3).map({case (id, pred, score) =>
//                        s"$id,$pred,$score,$now,?,?"
//                    }).mkString("\n"))
//                    out.close()
//                    println("Done!")
//                case Left(msg: String) => println(msg)
//            }
//        }
//    }
//
//    def printUsage() = {
//        println(usageMessage)
//    }
//}