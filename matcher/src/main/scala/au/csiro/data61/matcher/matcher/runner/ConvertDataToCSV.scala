package au.csiro.data61.matcher.matcher.runner

import au.csiro.data61.matcher.data._
import au.csiro.data61.matcher.matcher._
import au.csiro.data61.matcher.matcher.eval.datasetutils._
import au.csiro.data61.matcher.matcher.eval.metrics._
import au.csiro.data61.matcher.ingestion.loader._
import au.csiro.data61.matcher.nlptools.distance.LCSubsequenceDistanceMetric

import au.csiro.data61.matcher.matcher.train._

import java.io._

object ConvertDataToCSV {
    val usageMessage = """Usage:
                         #    java -jar prototype.jar <output-dir>
                       """.stripMargin('#')

    def main(args: Array[String]) = {
        if(args.size != 1) {
            printUsage()
        } else {
            val outputDirPath = args(0)
            val outputDir = new File(outputDirPath)
            if(!outputDir.exists) {
                outputDir.mkdir()
            }

            //load all our training data
            println("Loading datasets for training...")
            val northixLoader = new {val dummy = 1} with NorthixUtils
            val wiscLoader = new {val dummy = 1} with WISCRealEstateDomain1Utils
            val freebaseDbpediaLoader = new {val dummy = 1} with FreebaseDBPediaUtils
            val northixData = northixLoader.loadDataSets()
            val wiscData = wiscLoader.loadDataSets()
            val freebaseDbpediaData = freebaseDbpediaLoader.loadDataSets()
            val northixLabels = northixLoader.loadLabels()
            val wiscLabels = wiscLoader.loadLabels()
            val freebaseDbpediaLabels = freebaseDbpediaLoader.loadLabels()
            println("Datasets loaded.")

            List(northixData,wiscData,freebaseDbpediaData).foreach({datamodel => 
                writeDataModel(datamodel, outputDirPath, "")
            })

            val labels: List[BasicLabels] = List(northixLabels,wiscLabels,freebaseDbpediaLabels).map({
                case x: ContainsPositiveLabels => BasicLabels(x.positiveLabels)
            })
            val combinedLabels = labels.tail.foldLeft(labels.head)(
                (collection: BasicLabels, next: BasicLabels) => (collection ++ next).asInstanceOf[BasicLabels]
            )
            writeLabels(combinedLabels, outputDirPath)
        }
    }

    def writeDataModel(datasets: List[AbstractDataModel], outputDirPath: String, parentId: String): Unit = datasets.head match {
        case table: DataModel =>
            datasets.asInstanceOf[List[DataModel]].map({case table => writeDataModel(table.children.get, outputDirPath, table.metadata.get.name)})
        case attribute: Attribute =>
            val attributes = datasets.asInstanceOf[List[Attribute]]

            //build headers
            val headers = attributes.map({x => truncateSuffix(x.id)}).mkString(",")

            //build content
            val attrVals = attributes.map({_.values})
            val attrValsHead = attrVals.head.map({case v => List("\"" + cleanString(v) + "\"")})
            val attrValsCsv = attrVals.tail.foldLeft(attrValsHead)((head: List[List[String]], attrvals: List[String]) => {
                (head zip attrvals).map({case (l: List[String], e: String) => l :+ ("\"" + cleanString(e) + "\"")})
            })

            var out = new PrintWriter(new File(s"$outputDirPath/${parentId}.csv"))
            out.println(headers)
            out.println(attrValsCsv.map({_.mkString(",")}).mkString("\n"))
            out.close()
    }

    def writeLabels(labels: BasicLabels, outputDirPath: String): Unit = {
        var out = new PrintWriter(new File(s"$outputDirPath/labels.csv"))
        labels.positiveLabels.foreach({attrSet => out.println(attrSet.map(fixLabel).mkString(","))})
        out.close()
    }

    def truncateSuffix(attrId: String): String = {
        val nameRegex1 = "^(.+)@([^@]+)@([^@]+)$".r
        val nameRegex2 = "^([^@]+)@([^@]+)$".r
        attrId match {
            case nameRegex1(name,table,db) => name
            case nameRegex2(name,db) => name
        }
    }

    def fixLabel(label: String) = {
        // val labelRegex1 = "^(.+)@house_listing@(.+)$".r
        // val labelRegex2 = "^(.+)@(1.dat|2.txt)$".r
        label match {
            // case labelRegex1(a,b) => s"${a}@${b}.csv"
            // case labelRegex2(a,b) => s"${a}.csv"
            case x => s"${x}.csv"
        }
    }

    def cleanString(s: String): String = {
        s.replaceAll("\n", " ").replaceAll("\"", "'")
    }

    def printUsage() = {
        println(usageMessage)
    }   
}