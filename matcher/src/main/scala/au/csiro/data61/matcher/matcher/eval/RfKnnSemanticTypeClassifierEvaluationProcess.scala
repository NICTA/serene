//package au.csiro.data61.matcher.matcher.eval
//
//import scala.util._
//
//import au.csiro.data61.matcher.data._
//import au.csiro.data61.matcher.ingestion.loader._
//import au.csiro.data61.matcher.matcher.eval.EvalTypes._
//import au.csiro.data61.matcher.matcher.train._
//import au.csiro.data61.matcher.matcher.features._
//
//package object EvalTypes {
//    type PredictionWithGt = (String,String)
//}
//
//case class ConfusionMatrix(classList: List[String], labels: List[SemanticTypeLabel], predictions: List[(String, String)]) {
//    val table = buildMatrix(labels, predictions)
//
//    def buildMatrix(labels: List[SemanticTypeLabel], predictions: List[(String,String)]): Array[Array[Int]] = {
//        val predsWithGt = predictions.map({case (attrId, predictedClass) =>
//            val gt = labels.find((l: SemanticTypeLabel) => l.getId == attrId).map({_.getGtClass}).getOrElse("unknown")
//            (attrId, predictedClass, gt)
//        })
//
//        //debug only: save preds and gt into preds_gt.csv
//        // val out = new java.io.PrintWriter(new java.io.File("preds_gt.csv"))
//        // out.println(predsWithGt.map({case (attrId, pred, gt) => s"$attrId,$pred,$gt"}).mkString("\n"))
//        // out.close
//
//        val numClasses = classList.size
//        val m = Array.ofDim[Int](numClasses, numClasses)
//
//        predsWithGt.foreach({case (attrId, predictedClass, gt) =>
//            val predClassIdx = classList.indexOf(predictedClass)
//            val gtClassIdx = classList.indexOf(gt)
//            m(gtClassIdx)(predClassIdx) = m(gtClassIdx)(predClassIdx) + 1
//        })
//
//        m
//    }
//}
//
//case class RfKnnSemanticTypeClassifierEvaluationProcess() {
//    val useUnknownClass = true
//
//    def loadData(rawDataPath: String): DataModel = {
//        println("Loading raw data...")
//        val dataLoader = CsvDataLoader()
//        val rawData = dataLoader.load(rawDataPath)
//        println("Raw data loaded with " + DataModel.getAllAttributes(rawData).size + " attributes.")
//        rawData
//    }
//
//    def loadLabels(labelsPath: String): SemanticTypeLabels = {
//        println("Loading labels...")
//        val labelsLoader = SemanticTypeLabelsLoader()
//        val labels = labelsLoader.load(labelsPath)
//        println("Loaded " + labels.labelsMap.size + " labels.")
//        println("""    Default class for unlabelled instances will be "unknown".\n""")
//        labels
//    }
//
//
//    def startEvaluation(
//            classList: List[String],
//            dataPath: String,
//            labelsPath: String,
//            minTrainSetProp: Double,
//            trainSetPropIncrement: Double,
//            testSetProp: Double,
//            numRepetitions: Int,
//            resamplingStrategy: String,
//            featuresConfig: FeatureSettings,
//            costMatrixFile: Option[String],
//            outputPath: String) = {
//
//        //some sanity checks
//        var proceed = true
//        if(trainSetPropIncrement <= 0 || trainSetPropIncrement > 1) {
//            println("!!! ERROR: The training set proportion increment (" + trainSetPropIncrement + ") must be less in (0,1]. !!!")
//            proceed = false
//        }
//        if(!ClassImbalanceResampler.validResamplingStrategies.contains(resamplingStrategy)) {
//            println("!!! Resampling strategy " + resamplingStrategy + " is not recognized. !!!")
//            println("Choose from " + ClassImbalanceResampler.validResamplingStrategies.mkString(", "))
//            proceed = false
//        }
//
//        if(proceed) {
//            val data = loadData(dataPath)
//            val labels = loadLabels(labelsPath)
//
//            val allAttrs = DataModel.getAllAttributes(data)
//            val attrsWithLabels = allAttrs.map({case attr => (attr, labels.findLabel(attr.id))})
//            val attrsGroupedByClass = attrsWithLabels.groupBy({_._2})
//
//            //classes with instances < 2 will be excluded
//            val (enoughInstSet, notEnoughInstSet) = attrsGroupedByClass.partition({case (classname, instances) => instances.size > 1})
//            notEnoughInstSet.foreach({
//                case (classname, instances) => println(s"Class $classname has ${instances.size} instances.  Excluding as not enough instances to run experiments (min 2).")
//            })
//            val filteredClassList = enoughInstSet.map({_._1}).toSet
//            val orderedClassList = classList.filter({case c => filteredClassList.contains(c)})
//            val finalClassList = if(!orderedClassList.contains("unknown")) ("unknown" :: orderedClassList) else orderedClassList
//
//            println("Number of labelled instances per class:")
//            println(enoughInstSet.map({case (cls, cnt) => s"    $cls: ${cnt.size}"}).mkString("\n") + "\n")
//
//            val trainSettings = costMatrixFile.map({case path =>
//                println(s"*** Cost matrix provided: $path ***")
//                println(s"*** Note: column and row indices should correspond to the ff classes:" + finalClassList + "***\n")
//                TrainingSettings(resamplingStrategy, featuresConfig, Some(Left(path)))
//            }).getOrElse({
//                println("Cost matrix not provided")
//                TrainingSettings(resamplingStrategy, featuresConfig)
//            })
//
//            //we multiply then divide by 100 to work around floating point precision issues
//            val increments = (minTrainSetProp*100 to (100-testSetProp*100) by trainSetPropIncrement*100).map({_/100.0}).toList
//            val allResults = increments.zipWithIndex.map({case (trainSetProp, idx) =>
//                println("==========================================================================")
//                println(f"Running experiment with $trainSetProp%1.2f of training instances and test set proportion $testSetProp%1.2f")
//                val allRepsResult = (1 to numRepetitions).map({repIdx =>
//                    println(s"----------- Running Repetition #$repIdx ------------")
//                    val expResult = runExperiment(classList, data, labels, trainSetProp, testSetProp, trainSettings, repIdx, outputPath)
//                    computeTestPerformance(expResult, trainSetProp, testSetProp, repIdx, outputPath)
//                })
//                (trainSetProp, testSetProp, allRepsResult)
//            })
//            saveResults(allResults, outputPath)
//        }
//    }
//
//    def partitionIntoTrainAndTest(instancesGroupedByClass: Map[String,List[(String,Attribute)]], trainSetProp: Double, testSetProp: Double): List[(String,List[Attribute],List[Attribute])] = {
//        instancesGroupedByClass.map({case (classname, instances) =>
//            val numTrainInst = (Math.max(Math.floor(instances.size.toDouble * trainSetProp), 1)).toInt
//            val numTestInst = (Math.max(Math.floor(instances.size.toDouble * testSetProp), 1)).toInt
//            val shuffled = Random.shuffle(instances)
//            (classname, shuffled.take(numTrainInst).map({_._2}), shuffled.drop(numTrainInst).take(numTestInst).map({_._2}))
//        }).toList
//    }
//
//    //TODO: check how many unknowns are used -- maybe we should oversample?
//    def runExperiment(
//            classList: List[String],
//            data: DataModel,
//            labels: SemanticTypeLabels,
//            trainSetProp: Double,
//            testSetProp: Double,
//            trainSettings: TrainingSettings,
//            repIdx: Int,
//            outputPath: String): (List[String], List[(String, Array[Double])], List[SemanticTypeLabel]) = {
//
//        val allAttrs = DataModel.getAllAttributes(data)
//        val attrsGroupedByClass = allAttrs.map({case a => (labels.findLabel(a.id), a)}).groupBy({_._1})
//        val partitionsPerClass = partitionIntoTrainAndTest(attrsGroupedByClass, trainSetProp, testSetProp)
//
//        val allTrainInsts = partitionsPerClass.flatMap({_._2})
//        val allTestInsts = partitionsPerClass.flatMap({_._3})
//
//        val trainingInstances = DataModel.copy(data, None, allTrainInsts.map({_.id}).toSet)
//        val testInstances = DataModel.copy(data, None, allTestInsts.map({_.id}).toSet)
//        val trainLabels = SemanticTypeLabels(allTrainInsts.map({
//            case a => (a.id, ManualSemanticTypeLabel(a.id, labels.findLabel(a.id)))
//        }).toMap)
//        val testLabels =allTestInsts.map({case a => ManualSemanticTypeLabel(a.id, labels.findLabel(a.id))})
//
//        //train model
//        val includedClassesList = if(!classList.contains("unknown")) ("unknown" :: classList) else classList
//        val trainer = new TrainMlibSemanticTypeClassifier(includedClassesList)
//        val model = trainer.train(trainingInstances, trainLabels, trainSettings, None)
//
//        //predict on test instances
//        val predictions = model.predict(List(testInstances)).map({case (attr, classProbs, attrFeatures) =>
//            (attr.id, classProbs)
//        }).toList
//
//        //store raw predictions
//        val expOutputFolder = f"$outputPath/results-for-trainsize-$trainSetProp%1.2f-testsize-$testSetProp%1.2f-rep-$repIdx/"
//        val expDir = new java.io.File(expOutputFolder)
//        if(!expDir.exists) {
//            expDir.mkdir
//        }
//
//        val evalOutputDir = new java.io.File(s"$expOutputFolder")
//        if(!evalOutputDir.exists) {
//            evalOutputDir.mkdir
//        }
//        val out = new java.io.PrintWriter(new java.io.File(s"$expOutputFolder/raw_predictions.csv"))
//        out.println("attribute_id,gt," + includedClassesList.mkString(","))
//        val rawPredsWithGt = predictions.map({case (id, probs) => (id, labels.findLabel(id), probs)})
//        out.println(rawPredsWithGt.map({case (id, label, probs) => s"$id,$label," + probs.mkString(",")}).mkString("\n"))
//        out.close
//
//        (includedClassesList, predictions, testLabels)
//    }
//
//    def computeTestPerformance(
//            testSetResult: (List[String], List[(String, Array[Double])], List[SemanticTypeLabel]),
//            trainSetProp: Double,
//            testSetProp: Double,
//            repIdx: Int,
//            outputPath: String): List[(String, Double)] = {
//        //build confusion matrix
//        val classList = testSetResult._1
//        val preds = testSetResult._2.map({case (attrId, scores) =>
//            val maxIdx = (scores zipWithIndex).maxBy(_._1)._2
//            (attrId, classList(maxIdx))
//        })
//        val labels = testSetResult._3
//        val cm = ConfusionMatrix(classList, labels, preds)
//
//        //store confusion matrix
//        val expOutputFolder = f"$outputPath/results-for-trainsize-$trainSetProp%1.2f-testsize-$testSetProp%1.2f-rep-$repIdx/"
//        val expDir = new java.io.File(expOutputFolder)
//        if(!expDir.exists) {
//            expDir.mkdir
//        }
//        val out = new java.io.PrintWriter(new java.io.File(s"$expOutputFolder/confusion_matrix.csv"))
//        out.println("," + classList.mkString(","))
//        out.println((cm.table zip classList).map({case (preds, className) => className + "," + preds.mkString(",")}).mkString("\n"))
//        out.close
//
//        //accuracies per class
//        val numCorrectPredsPerClass = (0 until classList.size).map({idx =>
//            cm.table(idx)(idx)
//        })
//        val numTestInstancesPerClass = (0 until classList.size).map({idx =>
//            cm.table.map({row => row(idx)}).sum
//        })
//        (0 until classList.size).map({
//            idx => if(numTestInstancesPerClass(idx) == 0) {
//                (classList(idx), 0.0)
//            } else {
//                (classList(idx), numCorrectPredsPerClass(idx).toDouble / numTestInstancesPerClass(idx).toDouble)
//            }
//        }).toList
//    }
//
//    def saveResults(results: Seq[(Double, Double, Seq[List[(String, Double)]])], outputDir: String) = {
//        val f = new java.io.File(outputDir)
//        if(!f.exists) {
//            f.mkdir
//        }
//
//        val flattenedTuples = results.flatMap({case (trainSize, testSize, allRepsResult) =>
//            (allRepsResult.zipWithIndex).flatMap({case (classAccuracies, repIdx) =>
//                classAccuracies.map({case (className, accuracy) =>
//                    (trainSize, testSize, repIdx+1, className, accuracy)
//                })
//            })
//        })
//
//        //store detailed results
//        val tuplesStr = flattenedTuples.map({case (trainSize, testSize, repIdx, className, accuracy) =>
//            s"$trainSize,$testSize,$repIdx,$className,$accuracy"
//        }).mkString("\n")
//
//        val detailedPW = new java.io.PrintWriter(new java.io.File(f + "/results-detailed.csv"))
//        detailedPW.println("trainsize,testsize,repnum,class,accuracy")
//        detailedPW.println(tuplesStr)
//        detailedPW.close
//
//        //store results averaged per class
//        val resultsAveragedOverReps = flattenedTuples.groupBy({case x =>
//            (x._1, x._2, x._4)
//        }).toList.sortBy({case (k, v) => k._1}).map({case ((trainSize, testSize, className), values) =>
//            val mean = values.map({_._5}).sum.toDouble / values.size.toDouble
//            val variance = values.map({_._5 - mean}).map({case x => x*x}).sum / values.size.toDouble
//            f"$trainSize,$testSize,$className,$mean,$variance"
//        })
//
//        val avgOverRepsPW = new java.io.PrintWriter(new java.io.File(f + "/results-averaged-over-reps.csv"))
//        avgOverRepsPW.println("trainsize,testsize,class,accuracy_mean,accuracy_variance")
//        avgOverRepsPW.println(resultsAveragedOverReps.mkString("\n"))
//        avgOverRepsPW.close
//
//        //store results averaged over all classes
//        val resultsAveragedOverClasses = flattenedTuples.groupBy({case x =>
//            (x._1, x._2, x._3) //group by trainsize,testsize,repnum
//        }).map({case ((trainSize, testSize, repNum), values) =>
//            (trainSize, testSize, repNum, values.map({_._5}).sum.toDouble / values.size.toDouble) //class averaged accuracy
//        }).groupBy({
//            case x => (x._1, x._2) //group by trainsize,testsize
//        }).toList.sortBy({
//            case (k,v) => k._1
//        }).map({
//            case ((trainSize, testSize), values) =>
//                val avg = values.map({_._4}).sum.toDouble / values.size.toDouble
//                f"$trainSize%1.2f,$testSize%1.2f,$avg%1.4f," + values.map({case (_,_,_,v) => f"$v%1.2f"}).mkString("|")
//        })
//
//        val avgOverClassesPW = new java.io.PrintWriter(new java.io.File(f + "/results-averaged-over-classes.csv"))
//        avgOverClassesPW.println("trainsize,testsize,accuracy,breakdown")
//        avgOverClassesPW.println(resultsAveragedOverClasses.mkString("\n"))
//        avgOverClassesPW.close
//    }
//}
