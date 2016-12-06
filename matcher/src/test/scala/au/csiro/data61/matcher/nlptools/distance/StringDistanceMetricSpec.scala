package au.csiro.data61.matcher.nlptools.distance

import org.specs2._
import au.csiro.data61.matcher.nlptools.distance._

class StringDistanceMetricSpec extends mutable.Specification {
    """LCSubsequenceDistanceMetric computeDistance("address","emailaddress")""" should {
        s"return 7" in {
            LCSubsequenceDistanceMetric().computeLCSubsequence("emailaddress", "address") mustEqual 7
        }
        s"return 3" in {
            LCSubsequenceDistanceMetric().computeLCSubsequence("afoob", "bfozo") mustEqual 3
            LCSubsequenceDistanceMetric().computeLCSubsequence("afoob", "bfooz") mustEqual 3
        }
        s"return 0" in {
            LCSubsequenceDistanceMetric().computeLCSubsequence("afoob", "") mustEqual 0
            LCSubsequenceDistanceMetric().computeLCSubsequence("", "afoob") mustEqual 0
            LCSubsequenceDistanceMetric().computeLCSubsequence("z", "afoob") mustEqual 0
            LCSubsequenceDistanceMetric().computeLCSubsequence("afoob", "z") mustEqual 0
        }
        s"return low distances for similar words" in {
            LCSubsequenceDistanceMetric().computeDistance("custName", "customerName") mustEqual 0.19999999999999996
            LCSubsequenceDistanceMetric().computeDistance("shiptoaddress", "ship2address") mustEqual 0.12
        }
        s"return high distances for dissimilar words" in {
            LCSubsequenceDistanceMetric().computeDistance("phone", "gender") mustEqual 0.6363636363636364
            LCSubsequenceDistanceMetric().computeDistance("employee", "staff") mustEqual 1
        } 
    }
}