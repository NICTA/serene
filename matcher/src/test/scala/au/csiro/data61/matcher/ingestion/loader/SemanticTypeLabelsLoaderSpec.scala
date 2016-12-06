package au.csiro.data61.matcher.ingestion.loader

import org.specs2._
import au.csiro.data61.matcher.data._
import au.csiro.data61.matcher.ingestion.loader._

class SemanticTypeLabelsLoaderSpec extends mutable.Specification {
    val labelsFile = "src/test/resources/datasets/wisc/realestate1/semtype_labels.csv"
    val labelsFolder = "src/test/resources/datasets/wisc/realestate1/test-labels-loader"

    s"""SemanticTypeLabelsLoaderSpec load("$labelsFile")""" should {
        s"load and parse $labelsFile" in {
            val labels = SemanticTypeLabelsLoader().load(labelsFile)
            labels.labelsMap.size mustEqual 78
        }
    }

    s"""SemanticTypeLabelsLoaderSpec load("$labelsFolder")""" should {
        s"load and parse all files from $labelsFolder" in {
            val labels = SemanticTypeLabelsLoader().load(labelsFolder)
            labels.labelsMap.size mustEqual (78*2)
        }
    }
}