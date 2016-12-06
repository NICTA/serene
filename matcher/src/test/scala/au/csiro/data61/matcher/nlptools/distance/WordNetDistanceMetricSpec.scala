package au.csiro.data61.matcher.nlptools.distance

import org.specs2._
import au.csiro.data61.matcher.nlptools.distance._


class WordNetDistanceMetricSpec extends mutable.Specification {
	val expectedDist1 = 0.33333333333333337
    """WordNetDistanceMetricSpec computeDistance("address","residence")""" should {
        s"return $expectedDist1" in {
            WordNetDistanceMetric().computeDistance("address", "residence") mustEqual expectedDist1
        }
    }

    val expectedDist2 = 0.33333333333333337
    """WordNetDistanceMetricSpec computeDistance("staff","personnel")""" should {
        s"return $expectedDist2" in {
            WordNetDistanceMetric().computeDistance("staff", "personnel") mustEqual expectedDist2
        }
    }

    val expectedDist3 = 0.8024691358024691
    """WordNetDistanceMetricSpec computeDistance("employee","phone")""" should {
        s"return $expectedDist3" in {
            WordNetDistanceMetric().computeDistance("employee", "phone") mustEqual expectedDist3
        }
    }

    val expectedDist4 = 0.5555555555555556
    """WordNetDistanceMetricSpec computeDistance("sex","gender")""" should {
        s"return $expectedDist4" in {
            WordNetDistanceMetric().computeDistance("sex", "gender") mustEqual expectedDist4
            WordNetDistanceMetric().computeDistance("gender", "sex") mustEqual expectedDist4
        }
    }    

    val expectedDist5 = 0.33333333333333337
    """WordNetDistanceMetricSpec computeDistance("district","region")""" should {
        s"return $expectedDist5" in {
            WordNetDistanceMetric().computeDistance("district", "region") mustEqual expectedDist5
        }
    }

    //This distance seems too high!
    val expectedDist6 = 0.7037037037037037
    """WordNetDistanceMetricSpec computeDistance("location","address")""" should {
        s"return $expectedDist6" in {
            WordNetDistanceMetric().computeDistance("location", "address") mustEqual expectedDist6
        }
    }

    val expectedDist7 = 0.5555555555555556
    """WordNetDistanceMetricSpec computeDistance("size","dimension")""" should {
        s"return $expectedDist7" in {
            WordNetDistanceMetric().computeDistance("size", "dimension") mustEqual expectedDist7
        }
    }

    val expectedDist8 = 0.7037037037037037
    """WordNetDistanceMetricSpec computeDistance("house","lot")""" should {
        s"return $expectedDist8" in {
            WordNetDistanceMetric().computeDistance("house", "lot") mustEqual expectedDist8
        }
    }

    val expectedDist9 = 0
    """WordNetDistanceMetricSpec computeDistance("description","description")""" should {
        s"return $expectedDist9" in {
            WordNetDistanceMetric().computeDistance("description", "description") mustEqual expectedDist9
        }
    }   

    val expectedDist10 = 0.5555555555555556
    """WordNetDistanceMetricSpec computeDistance("occupation","job")""" should {
        s"return $expectedDist10" in {
            WordNetDistanceMetric().computeDistance("occupation", "job") mustEqual expectedDist10
        }
    }

    val expectedDist11 = 0.5555555555555556
    """WordNetDistanceMetricSpec computeDistance("occupation","job")""" should {
        s"return $expectedDist11" in {
            WordNetDistanceMetric().computeDistance("occupation", "job") mustEqual expectedDist11
        }
    }    

    val expectedDist12 = 0.5555555555555556
    """WordNetDistanceMetricSpec computeDistance("wage","salary")""" should {
        s"return $expectedDist12" in {
            WordNetDistanceMetric().computeDistance("wage", "salary") mustEqual expectedDist12
        }
    }        

    val expectedDist13 = 0.7037037037037037
    """WordNetDistanceMetricSpec computeDistance("nationality","citizenship")""" should {
        s"return $expectedDist13" in {
            WordNetDistanceMetric().computeDistance("nationality", "citizenship") mustEqual expectedDist13
        }
    }

    val expectedDist14 = 0.5555555555555556
    """WordNetDistanceMetricSpec computeDistance("department","section")""" should {
        s"return $expectedDist14" in {
            WordNetDistanceMetric().computeDistance("department", "section") mustEqual expectedDist14
        }
    }    
}