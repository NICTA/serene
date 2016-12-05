package au.csiro.data61.matcher.matcher.runner

import au.csiro.data61.matcher.data._
import au.csiro.data61.matcher.matcher._
import au.csiro.data61.matcher.matcher.eval.datasetutils._
import au.csiro.data61.matcher.matcher.eval.metrics._
import au.csiro.data61.matcher.ingestion.loader._
import au.csiro.data61.matcher.nlptools.distance.LCSubsequenceDistanceMetric

import au.csiro.data61.matcher.matcher.train._

import java.io._

object ConvertWISCDataToCSV {
    val usageMessage = """Usage:
                         #    java -jar prototype.jar <output-dir>
                       """.stripMargin('#')

    //classes with >= 3 instances
    val classList = List("cooling", "address", "firm_name", "email", "price", "type", "mls",
                         "levels", "office_phone", "agent_phone", "year_built", "garage", "fireplace",
                         "bathrooms", "size", "agent_name", "house_description", "heating",
                         "fax", "bedrooms", "water_coop", "school_district", "firm_location")

    val remapClasses = Map("office_phone" -> "phone", 
                           "agent_phone" -> "phone",
                           "fax" -> "phone")

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
            val wiscLoader = new {val dummy = 1} with WISCRealEstateDomain1Utils
            val wiscData = wiscLoader.loadDataSets()
            val wiscLabels = wiscLoader.loadLabels()
            println("Datasets loaded.")

            // println(wiscData)

            List(wiscData).foreach({datamodel => 
                writeDataModel(datamodel, outputDirPath, "")
            })

            val labels: List[BasicLabels] = List(wiscLabels).map({
                case x: ContainsPositiveLabels => BasicLabels(x.positiveLabels.filter({_.size >= 3})) //we just want classes with >= 3 instances
            })
            val allLabels = labels.tail.foldLeft(labels.head)((collection: BasicLabels, next: BasicLabels) => (collection ++ next).asInstanceOf[BasicLabels])

            //combine some labels
            val combinedLabels = allLabels.positiveLabels.zip(classList).map({
                case (insts, classname) => (insts, remapClasses.getOrElse(classname, classname))
            }).groupBy({_._2}).map({
                case (classname, groups) => (groups.flatMap({_._1}), classname)
            }).toList

            writeLabels(combinedLabels, outputDirPath)

            writeClasses(combinedLabels.map({_._2}), outputDirPath)
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

    def writeLabels(labels: List[(List[String], String)], outputDirPath: String): Unit = {
        var out = new PrintWriter(new File(s"$outputDirPath/labels.csv"))
        val labelsStr = labels.map({case (instances, className) =>
            instances.map(fixLabel).map({attrId => s"$attrId,$className"}).mkString("\n")
        }).mkString("\n")
        out.println("attr_id,class")
        out.println(labelsStr)
        out.close()
    }

    def writeClasses(classList: List[String], outputDirPath: String): Unit = {
        var out = new PrintWriter(new File(s"$outputDirPath/classes.csv"))
        out.println(classList.mkString("\n"))
        out.close()
    }

    def truncateSuffix(attrId: String): String = {
        val nameRegex = "^(.*)@([^@]+)$".r
        attrId match {
            case nameRegex(attr_name,filename) => attr_name
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