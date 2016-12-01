package com.nicta.dataint.matcher.eval.datasetutils

import org.specs2._
import com.nicta.dataint.matcher.eval._
import com.nicta.dataint.data._
import com.nicta.dataint.ingestion.loader._

class WISCRealEstateDomain1UtilsSpec extends mutable.Specification {
    s"""WISCRealEstateDomain1Utils loadSemanticTypeLabels()""" should {
        "load semantic type labels" in {
            val loader = new {val dummy = 1} with WISCRealEstateDomain1Utils
            val labels = loader.loadSemanticTypeLabels()
            labels.labelsMap.size mustEqual 78
        }
    }
}