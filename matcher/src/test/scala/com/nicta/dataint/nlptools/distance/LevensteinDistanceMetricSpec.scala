package com.nicta.dataint.nlptools.editdistance

import org.specs2._
import com.nicta.dataint.nlptools.distance._

class LevensteinDistanceMetricSpec extends mutable.Specification {
    """LevensteinDistanceMetric computeDistance("foobar","foabar")""" should {
        "return 1" in {
            LevensteinDistanceMetric.computeDistance("foobar", "foabar") mustEqual 1
        }
    }
}