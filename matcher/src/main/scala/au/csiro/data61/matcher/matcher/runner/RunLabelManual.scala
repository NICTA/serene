package au.csiro.data61.matcher.matcher.runner

import au.csiro.data61.matcher.matcher.interface.LabelManualInterface

object RunLabelManual {
    val usageMessage = """Usage:
                         #    java -jar prototype.jar <path-to-datasource> <path-to-label-file> <path-to-class-list>
                       """.stripMargin('#')

    def main(args: Array[String]) = {
        if(args.size != 3) {
            printUsage()
        } else {
            val dataSourcePath = args(0)
            val labelsPath = args(1)
            val classListPath = args(2)
            LabelManualInterface(dataSourcePath, labelsPath, classListPath).run()
        }
    }

    def printUsage() = {
        println(usageMessage)
    }
}