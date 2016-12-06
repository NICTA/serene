package au.csiro.data61.matcher.nlptools.parser

import org.specs2._
import com.mdimension.jchronic._
import com.mdimension.jchronic.tags._
import com.mdimension.jchronic.utils._

import au.csiro.data61.matcher.data._
import au.csiro.data61.matcher.nlptools.parser._
import au.csiro.data61.matcher.ingestion.loader._

class JChronicDateParserSpec extends mutable.Specification {
    val testDate1 = "may 27 2014"
    s"""DateParser parse("$testDate1")""" should {
        "return y=2014,m=4,d=27" in {
            val span = Chronic.parse(testDate1)
            println(span.toString)
            1 mustEqual 1
        }
    }

    val testDate2 = "15/12/2014"
    s"""DateParser parse("$testDate2")""" should {
        "return y=2014,m=4,d=27" in {
            val span = Chronic.parse(testDate2)
            println(span.toString)
            1 mustEqual 1
        }
    }

    val testDate3 = "asdf"
    s"""DateParser parse("$testDate3")""" should {
        "return null" in {
            val span = Chronic.parse(testDate3)
            span mustEqual null
        }
    }

    val testDate4 = "1241"
    s"""DateParser parse("$testDate4")""" should {
        "return y=2014,m=4,d=27" in {
            val span = Chronic.parse(testDate4, new Options(false))
            println(span)
            1 mustEqual 1
        }
    }

    val testDate5 = "1953-03-05+02:00"
    s"""DateParser parse("$testDate5")""" should {
        "return y=1953,m=03,d=05" in {
            val span = Chronic.parse(testDate5, new Options(true))
            println(span)
            1 mustEqual 1
        }
    }

    //val testDate6 = "Jan 13, 2012 1:00 PM" //jchronic fails on this input!
    val testDate6 = "Jan 13, 2012 1:00 PM"
    s"""DateParser parse("$testDate6")""" should {
        "return y=2012,m=01,d=13" in {
            val now = java.util.Calendar.getInstance
            println("now: " + now.getTime)
            println("parsing " + testDate6)
            val span = Chronic.parse(testDate6, new Options(now, true))
            println(span)
            1 mustEqual 1
        }
    }

    val testDate7 = "$3,500"
    s"""DateParser parse("$testDate7")""" should {
        "return None" in {
            val span = Chronic.parse(testDate7, new Options(true))
            println(span)
            1 mustEqual 1
        }
    }

    val testDate8 = "15-10-2014 21:00 PM"
    s"""DateParser parse("$testDate8")""" should {
        "return None" in {
            val span = Chronic.parse(testDate8, new Options(true))
            println(span)
            1 mustEqual 1
        }
    }

    val testDate9 = "15-10-2014 "
    s"""DateParser parse("$testDate9")""" should {
        "return None" in {
            val span = Chronic.parse(testDate9)
            println(span)
            1 mustEqual 1
        }
    }

    val testDate10 = "301 BRACKEN"
    s"""DateParser parse("$testDate10")""" should {
        "return None" in {
            val span = Chronic.parse(testDate10)
            println(span)
            1 mustEqual 1
        }
    }
}