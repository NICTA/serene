package au.csiro.data61.matcher.ingestion.loader

import au.csiro.data61.matcher.data.LabelTypes._
import au.csiro.data61.matcher.data._
import scala.io.Source

import java.io._

case class PositiveOnlyLabelsLoader() {
    def load(path: String): BasicLabels = {
        BasicLabels(Source.fromFile(path).getLines.filter({case l => !(l.startsWith("#") || l.trim.length == 0)}).map(_.split(",").toSet).toList)
    }
}

case class PosAndAmbigLabelsLoader() {
    val ambigLabelsRegex = """\(\(([^()]*)\),\(([^()]*)\)\)""".r

    def load(posLabelsPath: String, ambigLabelsPath: String): PosAndAmbigLabels = {
    	val posLabels = Source.fromFile(posLabelsPath).getLines.filter({case l => !(l.startsWith("#") || l.trim.length == 0)}).map(_.split(",").toSet).toList
        val ambigLabels = Source.fromFile(ambigLabelsPath).getLines.filter({case l => !(l.startsWith("#") || l.trim.length == 0)}).map({
        	case ambigLabelsRegex(s1,s2) => {
        		(s1.split(",").distinct.toSet, s2.split(",").distinct.toSet)
            }
            case _ => {
            	(Set[String](),Set[String]())
            }
        }).toList
        PosAndAmbigLabels(posLabels,ambigLabels)
    }	
}

case class SemanticTypeLabelsLoader() {
    val excludedFilesRegex = "^(\\..*)$".r

    def load(labelsPath: String): SemanticTypeLabels = {
        loadRecursive(new File(labelsPath))
    }

    def loadRecursive(file: File): SemanticTypeLabels = {
        if(file.isDirectory()) {
            val files = file.listFiles.filter({f =>
                val fnameOnly = f.getPath.substring(f.getPath.lastIndexOf("/")+1, f.getPath.length)
                fnameOnly match {
                    case excludedFilesRegex(name) => false
                    case n => {
//                        println("reading labels file: " + n)
                        true
                    }
                }
            })
            files.map(loadRecursive).foldLeft(SemanticTypeLabels(Map()))((a,b) => (a ++ b).asInstanceOf[SemanticTypeLabels])
        } else {
            println("reading " + file.getPath)
            SemanticTypeLabels(
                Source.fromFile(file).getLines.drop(1).map({case line =>
                    val tokens = line.split(",")
                    if(tokens.size == 2) {
                        Some((tokens(0), ManualSemanticTypeLabel(tokens(0), tokens(1))))
                    } else
                    if(tokens.size == 6) {
                        if(tokens(4) == "?") {
                            None //Do not consider predictions which haven't been validated yet.
                        } else {
                            Some((tokens(0), PredictedSemanticTypeLabel(tokens(0),tokens(1),tokens(2).toDouble,tokens(3),tokens(4),tokens(5))))    
                        }
                    } else {
                        None
                    }
                }).filter({
                    case Some(x) => true
                    case _ => false
                }).map(_.get).toMap
            )
        }
    }
}