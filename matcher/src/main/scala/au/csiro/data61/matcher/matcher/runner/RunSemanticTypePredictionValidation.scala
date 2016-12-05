package au.csiro.data61.matcher.matcher.runner

import scala.io._

import java.io._

/**
 *  This class is called by the dirstruct/semantic_type_classifier/validate_predictions.sh script.  
 *  For more info on the validation process, please see the VALIDATION section in
 *  dirstruct/semantic_type_classifier/HOWTO.
 **/
object RunSemanticTypePredictionValidation {
    type Prediction = (String,String,Double,String,String,String)
    val dateFormat = new java.text.SimpleDateFormat("yyyy/MM/dd")

    val usageMessage = """Usage:
                         #    java -cp prototype.jar au.csiro.data61.matcher.matcher.runner.RunSemanticTypePredictionValidation [options] <path-to-predictions-file>
                         # 
                         #Options:
                         #    -i <path_to_class_list>:  Run interactive mode.
                         #    -ag <threshold> : Accept all predictions with confidence >= threshold.
                         #    -rl <threshold> : Reject all predictions with confidence < threshold.
                       """.stripMargin('#')

    val interactiveModeCommands = List(
        ("h", List(), "Show this help message."),
        ("sd", List(), "Show predictions sorted by decreasing confidence."),
        ("si", List(), "Show predictions sorted by increasing confidence."),
        ("c", List(), "List classes."),
        ("a", List("<index>"), "Accept prediction."),
        ("r", List("<index>", "<correct_class>"), "Reject prediction and assign correct_class as label."),
        ("ag", List("<threshold>"), "Accept all predictions with confidence >= threshold."),
        ("rl", List("<threshold>"), "Reject all predictions with confidence < threshold."),
        ("x", List(), "Save and exit."),
        ("q", List(), "Discard changes and exit.")
    )

    case class PredictionsPager(val predictions: List[Prediction], val pageSize: Int = 15) {
        var cursor = 0
        def getNext(): List[(Int,Prediction)] = {
            val endIdx = Math.min(cursor+pageSize, predictions.size)
            val batch = (cursor until endIdx) zip predictions.slice(cursor, endIdx)
            cursor = endIdx
            batch.toList
        }
        def hasNext(): Boolean = {
            return (cursor < predictions.size)
        }
    }

    def main(args: Array[String]) = {
        if(args.size == 0) {
            printUsage()
        } else {
            val firstArg = args(0)
            if(firstArg.startsWith("-")) {
                //user provided options
                if(firstArg.equals("-i")) {
                    //running in interactive mode
                    if(args.size < 2) printUsage()
                    else {
                        printInteractiveModeCommands()
                        runInteractive(args(1), args(2))
                    }
                } else
                if(firstArg.equals("-ag")) {
                    if(args.size < 2) printUsage()
                    else {
                        //accept all gt or above threshold
                        val threshold = args(1).toDouble
                        val predsPath = args(2)
                        println("Accepting all predictions with confidence >= " + threshold)

                        val preds = Source.fromFile(predsPath).getLines.drop(1).map({csv =>
                            val toks = csv.split(",")
                            (toks(0), toks(1), toks(2).toDouble, toks(3), toks(4), toks(5))
                        }).toList

                        val now = dateFormat.format(java.util.Calendar.getInstance.getTime)
                        if(threshold > 1 || threshold < 0) {
                            println("Threshold must be within the interval [0,1].")
                        } else {
                            val updatedPreds = preds.map({case (id, predLbl, conf, predDate, actualLbl, valDate) =>
                                if(conf >= threshold) {
                                    (id, predLbl, conf, predDate, predLbl, now)
                                } else {
                                    (id, predLbl, conf, predDate, actualLbl, valDate)
                                }
                            })

                            val out = new PrintWriter(new File(predsPath))
                            out.println("attribute_id,predicted_class,confidence,date_predicted,actual_class,date_validated")
                            out.println(updatedPreds.map({
                                case (id, predClass, confidence, datePred, actualClass, dateVal) => s"$id,$predClass,$confidence,$datePred,$actualClass,$dateVal"
                            }).mkString("\n"))
                            out.close()
                        }
                    }
                } else
                if(firstArg.equals("-rl")) {
                    if(args.size < 2) printUsage()
                    else {
                        //reject all below threshold
                        val threshold = args(1).toDouble
                        val predsPath = args(2)
                        println("Rejecting all predictions with confidence < " + threshold)

                        val preds = Source.fromFile(predsPath).getLines.drop(1).map({csv =>
                            val toks = csv.split(",")
                            (toks(0), toks(1), toks(2).toDouble, toks(3), toks(4), toks(5))
                        }).toList

                        val now = dateFormat.format(java.util.Calendar.getInstance.getTime)
                        if(threshold > 1 || threshold < 0) {
                            println("Threshold must be within the interval [0,1].")
                        } else {
                            val updatedPreds = preds.filter({case (_, _, conf: Double, _, _, _) => {(conf >= threshold)}})

                            val out = new PrintWriter(new File(predsPath))
                            out.println("attribute_id,predicted_class,confidence,date_predicted,actual_class,date_validated")
                            out.println(updatedPreds.map({
                                case (id, predClass, confidence, datePred, actualClass, dateVal) => s"$id,$predClass,$confidence,$datePred,$actualClass,$dateVal"
                            }).mkString("\n"))
                            out.close()
                        }
                    }
                }
            } else {
                printUsage()
            }
        }
    }

    def printInteractiveModeCommands() {
        val msg = interactiveModeCommands.map({case (command, args, desc) =>
            if(args.size > 0) s"""    $command ${args.mkString(" ")}: $desc"""
            else s"""    $command:\t$desc"""
        }).mkString("\n")
        println(msg)
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

    def runInteractive(classListPath: String, predsPath: String) = {
        val classes = "unknown" :: loadClassesRecursively(classListPath)
        val preds = Source.fromFile(predsPath).getLines.drop(1).map({csv =>
            val toks = csv.split(",")
            (toks(0), toks(1), toks(2).toDouble, toks(3), toks(4), toks(5))
        }).toList
        val br = new BufferedReader(new InputStreamReader(System.in));
        val validatedPreds = readAndProcessCommand(br, classes, preds, None)
        val msg = validatedPreds.map({predsToSave =>
            val out = new PrintWriter(new File(predsPath))
            out.println("attribute_id,predicted_class,confidence,date_predicted,actual_class,date_validated")
            out.println(predsToSave.map({
                case (id, predClass, confidence, datePred, actualClass, dateVal) => s"$id,$predClass,$confidence,$datePred,$actualClass,$dateVal"
            }).mkString("\n"))
            out.close()
            "Saved."
        }).getOrElse("Changes discarded.")
        println(msg)
    }

    def readAndProcessCommand(br: BufferedReader, classes: List[String], preds: List[Prediction], pager: Option[PredictionsPager]): Option[List[Prediction]] = {
        println("Enter Command: ")
        val command = br.readLine()
        if(command == "x") {
            Some(preds)
        } else
        if(command == "q") {
            None
        } else {
            if(command == "h") {
                printInteractiveModeCommands()
                readAndProcessCommand(br, classes, preds, None) //reset any pager
            }
            if(command == "c") {
                println(classes.mkString(", "))
                readAndProcessCommand(br, classes, preds, pager)
            } else
            if(command == "sd" || command == "si") {
                val pager = if(command == "sd") {
                    PredictionsPager(preds.sortBy(-_._3))
                } else {
                    PredictionsPager(preds.sortBy(_._3))
                }
                println(pager.getNext.map({case (idx,(id, predClass, confidence, datePred, actualClass, dateVal)) =>
                    s"$idx: $id,$predClass,$confidence,$datePred,$actualClass,$dateVal"
                }).mkString("\n"))
                if(pager.hasNext) println("    Type 'm' to see more...")
                readAndProcessCommand(br, classes, preds, Some(pager))
            } else
            if(command.startsWith("a ")) {
                val toks = command.split(" ")
                if(toks.size != 2) {
                    println("Invalid command")
                    readAndProcessCommand(br, classes, preds, pager)
                } else {
                    val idx = toks(1).toInt
                    if(idx >= 0 && idx < preds.size) {
                        val now = dateFormat.format(java.util.Calendar.getInstance.getTime)
                        val newEntry = preds(idx).copy(_5 = preds(idx)._2, _6 = now)
                        val updatedPreds = preds.updated(idx, newEntry)
                        println("    updated " + newEntry)
                        readAndProcessCommand(br, classes, updatedPreds, None) //reset any pager
                    } else {
                        println("Invalid index.")
                        readAndProcessCommand(br, classes, preds, None) //reset any pager
                    }
                }
            } else
            if(command.startsWith("r ")) {
                val toks = command.split(" ")
                if(toks.size != 3) {
                    println("Invalid command")
                    readAndProcessCommand(br, classes, preds, pager)
                } else {
                    val idx = toks(1).toInt
                    val newLabel = toks(2)
                    if(!classes.contains(newLabel)) {
                        println("Invalid class.  Please select one from the ff. or update your class list file.")
                        println(classes)
                        readAndProcessCommand(br, classes, preds, None) //reset any pager
                    } else
                    if(idx >= 0 && idx < preds.size) {
                        val now = dateFormat.format(java.util.Calendar.getInstance.getTime)
                        val newEntry = preds(idx).copy(_5 = newLabel, _6 = now)
                        val updatedPreds = preds.updated(idx, newEntry)
                        println("    updated " + newEntry)
                        readAndProcessCommand(br, classes, updatedPreds, None) //reset any pager
                    } else {
                        println("Invalid index.")
                        readAndProcessCommand(br, classes, preds, None) //reset any pager
                    }
                }
            } else
            if(command.startsWith("ag ")) {
                val toks = command.split(" ")
                if(toks.size != 2) {
                    println("Invalid command.")
                    readAndProcessCommand(br, classes, preds, pager)
                } else {
                    val thresh = toks(1).toDouble
                    val now = dateFormat.format(java.util.Calendar.getInstance.getTime)
                    if(thresh > 1 || thresh < 0) {
                        println("Threshold must be within the interval [0,1].")
                        readAndProcessCommand(br, classes, preds, pager)
                    } else {
                        val updatedPreds = preds.map({case (id, predLbl, conf, predDate, actualLbl, valDate) =>
                            if(conf >= thresh) {
                                (id, predLbl, conf, predDate, predLbl, now)
                            } else {
                                (id, predLbl, conf, predDate, actualLbl, valDate)
                            }
                        })
                        readAndProcessCommand(br, classes, updatedPreds, pager)
                    }
                }
            } else
            if(command.startsWith("rl ")) {
                val toks = command.split(" ")
                if(toks.size != 2) {
                    println("Invalid command.")
                    readAndProcessCommand(br, classes, preds, pager)
                } else {
                    val thresh = toks(1).toDouble
                    val now = dateFormat.format(java.util.Calendar.getInstance.getTime)
                    if(thresh > 1 || thresh < 0) {
                        println("Threshold must be within the interval [0,1].")
                        readAndProcessCommand(br, classes, preds, pager)
                    } else {
                        val updatedPreds = preds.map({case (id, predLbl, conf, predDate, actualLbl, valDate) =>
                            if(conf < thresh) {
                                (id, predLbl, conf, predDate, "unknown", now)
                            } else {
                                (id, predLbl, conf, predDate, actualLbl, valDate)
                            }
                        })
                        readAndProcessCommand(br, classes, updatedPreds, pager)
                    }
                }
            } else
            if(command == "m") {
                pager match {
                    case Some(p) =>
                        println(p.getNext.map({case (idx,(id, predClass, confidence, datePred, actualClass, dateVal)) =>
                            s"$idx: $id,$predClass,$confidence,$datePred,$actualClass,$dateVal"
                        }).mkString("\n"))
                        if(p.hasNext) println("    Type 'm' to see more...")
                        readAndProcessCommand(br, classes, preds, Some(p))
                    case None =>
                        println("Invalid command.")
                        readAndProcessCommand(br, classes, preds, pager)
                }
            } else {
                readAndProcessCommand(br, classes, preds, pager)
            }
        }
    }

    def printUsage() = {
        println(usageMessage)
    }
}