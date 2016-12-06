package au.csiro.data61.matcher.nlptools.editdistance

import org.specs2._
import au.csiro.data61.matcher.nlptools.distance._

class LevensteinDistanceMetricSpec extends mutable.Specification {
    """LevensteinDistanceMetric computeDistance("foobar","foabar")""" should {
        "return 1" in {
            LevensteinDistanceMetric.computeDistance("foobar", "foabar") mustEqual 1
        }
    }
}