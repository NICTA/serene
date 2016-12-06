package au.csiro.data61.matcher.nlptools.parser

import org.specs2._
import com.joestelmach.natty._

import au.csiro.data61.matcher.data._
import au.csiro.data61.matcher.nlptools.parser._
import au.csiro.data61.matcher.ingestion.loader._

import scala.collection.JavaConverters._

class NattyDateParserSpec extends mutable.Specification {
    val nattyParser = new Parser()

    val testDate1 = "may 27 2014"
    s"""DateParser parse("$testDate1")""" should {
        "return y=2014,m=4,d=27" in {
            val span = nattyParser.parse(testDate1)
            println(span.asScala.head.getDates.asScala.head)
            1 mustEqual 1
        }
    }

    val testDate2 = "15/12/2014"
    s"""DateParser parse("$testDate2")""" should {
        "return y=2014,m=4,d=27" in {
            val span = nattyParser.parse(testDate2)
            println(span.asScala.head.getDates.asScala.head)
            1 mustEqual 1
        }
    }

    // val testDate3 = "asdf"
    // s"""DateParser parse("$testDate3")""" should {
    //     "return null" in {
    //         val span = Chronic.parse(testDate3)
    //         span mustEqual null
    //     }
    // }

    val testDate4 = "1241"
    s"""DateParser parse("$testDate4")""" should {
        "return y=2014,m=4,d=27" in {
            val span = nattyParser.parse(testDate4)
            println(span.asScala.head.getDates.asScala.head)
            1 mustEqual 1
        }
    }

    val testDate5 = "1953-03-05+02:00"
    s"""DateParser parse("$testDate5")""" should {
        "return y=1953,m=03,d=05" in {
            val span = nattyParser.parse(testDate5)
            println(span.asScala.head.getDates.asScala.head)
            1 mustEqual 1
        }
    }

    val testDate6 = "Jan 13, 2012 1:00 PM"
    s"""DateParser parse("$testDate6")""" should {
        "return y=2012,m=01,d=13" in {
            val span = nattyParser.parse(testDate6)
            println(span.asScala.head.getDates.asScala.head)
            1 mustEqual 1
        }
    }

    val testDate7 = "$3,500"
        s"""DateParser parse("$testDate7")""" should {
        "return None" in {
            val span = nattyParser.parse(testDate7)
            println(span.asScala.head.getDates.asScala.head)
            1 mustEqual 1
        }
    }
}