package au.csiro.data61.matcher.matcher.runner

import au.csiro.data61.matcher.matcher.interface.InteractiveTypeClassifierInterface

object RunInteractiveTypeClassifier {
    val usageMessage = """Usage:
                         #    java -jar prototype.jar <path-to-data> <path-to-model> <path-to-labels> <path-to-class-list>
                       """.stripMargin('#')

    def main(args: Array[String]) = {
        if(args.size != 4) {
            printUsage()
        } else {
            val dataPath = args(0)
            val labelsPath = args(1)
            val modelPath = args(2)
            val classListPath = args(3)
            
            InteractiveTypeClassifierInterface(dataPath, modelPath, labelsPath, classListPath).run()
        }
    }

    def printUsage() = {
        println(usageMessage)
    }
}