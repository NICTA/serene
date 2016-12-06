package au.csiro.data61.matcher.nlptools.distance

import org.specs2._
import au.csiro.data61.matcher.nlptools.distance._


class WS4JWordNetDistanceMetricSpec extends mutable.Specification {
    val sourceData1 = List("customer","address")
    val targetData1 = List("client","location")
    s"""WS4JWordNetDistanceMetricSpec for $sourceData1 and $targetData1""" should {
        s"return correct matching" in {
            val bestAlignment = WS4JWordNetDistanceMetric().computeBestAlignment(sourceData1, targetData1)
            bestAlignment mustEqual List(("address","location",0.17647058823529416), ("customer","client",0.0))
        }
    }

    val sourceData2 = List("house","description")
    val targetData2 = List("description","home","extra")
    s"""WS4JWordNetDistanceMetricSpec for $sourceData2 and $targetData2""" should {
        s"return correct matching" in {
            val bestAlignment = WS4JWordNetDistanceMetric().computeBestAlignment(sourceData2, targetData2)
            bestAlignment mustEqual List(("description","description",0.0), ("house","home",0.0))
        }
    }    

    val expectedDist3 = 0.08823529411764708
    val sourceData3 = "customer_address"
    val targetData3 = "clientlocation"
    s"""WS4JWordNetDistanceMetricSpec for $sourceData3 and $targetData3 should be $expectedDist3""" should {
        s"return $expectedDist3" in {
            WS4JWordNetDistanceMetric().computeDistance(sourceData3, targetData3) mustEqual expectedDist3
        }
    }   
}