package au.csiro.data61.matcher.matcher.eval.datasetutils

import org.specs2._
import au.csiro.data61.matcher.matcher.eval._
import au.csiro.data61.matcher.data._
import au.csiro.data61.matcher.ingestion.loader._

class WISCRealEstateDomain1UtilsSpec extends mutable.Specification {
    s"""WISCRealEstateDomain1Utils loadSemanticTypeLabels()""" should {
        "load semantic type labels" in {
            val loader = new {val dummy = 1} with WISCRealEstateDomain1Utils
            val labels = loader.loadSemanticTypeLabels()
            labels.labelsMap.size mustEqual 78
        }
    }
}