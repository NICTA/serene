package au.csiro.data61.matcher.matcher.runner

import au.csiro.data61.matcher.ingestion.loader._
import au.csiro.data61.matcher.matcher.transformation._

import scala.io._

import java.io._


/**
 *  This class is called by the dirstruct/semantic_type_classifier/transform.sh script.  
 *  For more info on the transformation process, please see the TRANSFORMATION section in
 *  dirstruct/semantic_type_classifier/HOWTO.
 **/
object RunSemanticTypeDataTransformations {
    val usageMessage = """Usage:
                         #    java -cp prototype.jar au.csiro.data61.matcher.matcher.runner.RunSemanticTypeDataTransformations <path-to-raw-data> <path-to-labels> <path-to-transformations> <path-to-output-folder>
                       """.stripMargin('#')

    def main(args: Array[String]) = {
        if(args.size != 4) {
            printUsage()
        } else {
            val rawDataPath = args(0)
            val labelsPath = args(1)
            val transformationsPath = args(2)
            val outputPath = args(3)

            println("Loading raw data...")
            val dataLoader = CsvDataLoader()
            val rawData = dataLoader.load(rawDataPath)
            println("Raw data loaded.")

            println("Loading labels...")
            val labelsLoader = SemanticTypeLabelsLoader()
            val labels = labelsLoader.load(labelsPath)
            println("Loaded " + labels.labelsMap.size + " labels.")

            println("Loading transformations...")
            val transformations = loadTransformationsRecursively(transformationsPath)
            println(transformations.size + " transformations loaded.")

            println("Applying transformations...")
            val transformedData = DataModelTransformation.apply(rawData, transformations, labels)
            println("Finished applying transformations.")

            println("Saving data...")
            CSVDataWriter().writeDataModel(List(transformedData), outputPath)
            println("Done!")
        }
    }

    def loadTransformationsRecursively(path: String): List[Transformation] = {
        val f = new File(path)
        if(f.isDirectory) {
            val children = f.listFiles
            children.flatMap({case c => loadTransformationsRecursively(c.getPath)}).toList
        } else {
            Source.fromFile(f).getLines.map({case s => 
                val toks = s.split(",")
                Transformation(toks(0), toks(1), toks(2))
            }).toList
        }
    }

    def printUsage() = {
        println(usageMessage)
    }
}