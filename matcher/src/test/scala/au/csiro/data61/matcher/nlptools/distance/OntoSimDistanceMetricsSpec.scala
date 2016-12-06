package au.csiro.data61.matcher.nlptools.editdistance

import org.specs2._
import au.csiro.data61.matcher.nlptools.distance._

class OntoSimDistanceMetricsSpec extends mutable.Specification {
    """OntoSim NeedlemanWunschDistance computeDistance("foobar","foabar")""" should {
        "return 0.16666666666666666" in {
            OntoSimDistanceMetrics.computeDistance("NeedlemanWunschDistance")("foobar", "foabar") mustEqual 0.16666666666666666
        }
    }

    """OntoSim NeedlemanWunschDistance computeDistance("awefewa","1512321")""" should {
        "return 1" in {
            OntoSimDistanceMetrics.computeDistance("NeedlemanWunschDistance")("awefewa", "1512321") mustEqual 1
        }
    }

    """OntoSim NGramDistance computeDistance(" asdfqwer  auipr ", "  auipr  asdfqwer")""" should {
        "return 0" in {
            OntoSimDistanceMetrics.computeDistance("NGramDistance")(" asdfqwer  auipr ", "  auipr  asdfqwer") mustEqual 0
        }
    }

    """OntoSim NGramDistance computeDistance("id", "st")""" should {
        "return 0" in {
            OntoSimDistanceMetrics.computeDistance("NGramDistance")("id", "st") mustEqual 0
        }
    }
}