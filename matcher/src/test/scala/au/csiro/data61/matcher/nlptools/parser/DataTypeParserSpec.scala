package au.csiro.data61.matcher.nlptools.parser

import org.specs2._
import com.mdimension.jchronic._
import com.mdimension.jchronic.tags._
import com.mdimension.jchronic.utils._

import au.csiro.data61.matcher.data._
import au.csiro.data61.matcher.nlptools.parser._
import au.csiro.data61.matcher.ingestion.loader._

class DateParserSpec extends mutable.Specification {
	val testDate1 = "may 27 2014"
    s"""DateParser parse("$testDate1")""" should {
        "return y=2014,m=4,d=27" in {
        	val (y,m,d) = DateParser.parse(testDate1) match {
        		case Some(Date(y,m,d)) => (y,m,d)
        	}
            y mustEqual 2014
            m mustEqual 4
            d mustEqual 27
        }
    }

    val testDate2 = "asdf"
    s"""DateParser parse("$testDate2")""" should {
        "return None" in {
        	DateParser.parse(testDate2) mustEqual None
        }
    }

    // val testDate3 = "1953-03-05+02:00"
    // s"""DateParser parse("$testDate3")""" should {
    //     "not return None" in {
    //         DateParser.parse(testDate3) mustNotEqual None
    //     }
    // }

    //JChronic parser tests
    s"""DateParser parse("$testDate1")""" should {
        "return y=2014,m=4,d=27" in {
            val (y,m,d) = DateParser.parseWithJChronic(testDate1) match {
                case Some(Date(y,m,d)) => (y,m,d)
            }
            y mustEqual 2014
            m mustEqual 4
            d mustEqual 27
        }
    }

    s"""DateParser parse("$testDate2")""" should {
        "return None" in {
            DateParser.parseWithJChronic(testDate2) mustEqual None
        }
    }

    // NOTE: Semantic date parsers fails the test below.  It tries
    // to parse the floating point into some hour:minute:seconds value.
    // val testDate4 = "123.456"
    // s"""DateParser parse("$testDate4")""" should {
    //     "return None" in {
    //     	DateParser.parse(testDate4) mustEqual None
    //     }
    // }
}

class DataTypeParserSpec extends mutable.Specification {
    val testStr1 = "12.345"
    s"""DataTypeParser inferDataType("$testStr1")""" should {
        "return Float" in {
        	val valType = DataTypeParser.inferDataType(testStr1)
            valType mustEqual "Float"
        }
    }

    val testStr2 = "June 22, 1984"
    s"""DataTypeParser inferDataType("$testStr2")""" should {
        "return Date" in {
        	val valType = DataTypeParser.inferDataType(testStr2)
            valType mustEqual "Date"
        }
    }

    val testStr3 = "-1234"
    s"""DataTypeParser inferDataType("$testStr3")""" should {
        "return Integer" in {
        	val valType = DataTypeParser.inferDataType(testStr3)
            valType mustEqual "Integer"
        }
    }

    val testStr4 = "The quick brown fox."
    s"""DataTypeParser inferDataType("$testStr4")""" should {
        "return Integer" in {
        	val valType = DataTypeParser.inferDataType(testStr4)
            valType mustEqual "String"
        }
    }

    val testStr5 = "61430440622"
    s"""DataTypeParser inferDataType("$testStr5")""" should {
        "return Integer" in {
            val valType = DataTypeParser.inferDataType(testStr5)
            valType mustEqual "Long"
        }
    }

    val testStr6 = "0430440622"
    s"""DataTypeParser inferDataType("$testStr6")""" should {
        "return Integer" in {
            val valType = DataTypeParser.inferDataType(testStr6)
            valType mustEqual "Integer"
        }
    }


    s"""DataTypeParser inferDataType() on values T/F,0/1,Y/N""" should {
        "return Boolean" in {
            DataTypeParser.inferDataType("T") mustEqual "Boolean"
            DataTypeParser.inferDataType("F") mustEqual "Boolean"
            DataTypeParser.inferDataType("t") mustEqual "Boolean"
            DataTypeParser.inferDataType("f") mustEqual "Boolean"
            DataTypeParser.inferDataType("Y") mustEqual "Boolean"
            DataTypeParser.inferDataType("N") mustEqual "Boolean"
            DataTypeParser.inferDataType("y") mustEqual "Boolean"
            DataTypeParser.inferDataType("n") mustEqual "Boolean"
            DataTypeParser.inferDataType("0") mustEqual "Boolean"
            DataTypeParser.inferDataType("1") mustEqual "Boolean"
        }
    }

    s"""DataTypeParser inferDataType() on date, time, datetime""" should {
        "return either date, time, datetime" in {
            DataTypeParser.inferDataType("1:00pm") mustEqual "Time"
            DataTypeParser.inferDataType("100pm") mustEqual "Time"
            DataTypeParser.inferDataType("2015") mustEqual "Date"
            DataTypeParser.inferDataType("Jan 13, 2012") mustEqual "Date"
            DataTypeParser.inferDataType("15/10/2014") mustEqual "Date"
            DataTypeParser.inferDataType("15-10-2014") mustEqual "Date"
            DataTypeParser.inferDataType("January 13, 2012") mustEqual "Date"
            DataTypeParser.inferDataType("January 13, 2012 12:00AM") mustEqual "DateTime"
            DataTypeParser.inferDataType("15-10-2014 21:00 PM") mustEqual "DateTime"
            DataTypeParser.inferDataType("21:00 PM 15-10-2014") mustEqual "DateTime"
        }
    }

    s"""DataTypeParser inferDataType() on date, time, datetime""" should {
        "not return either date, time, datetime" in {
            DataTypeParser.inferDataType("02010") mustEqual "Integer"
            DataTypeParser.inferDataType("febr") mustEqual "String"
            DataTypeParser.inferDataType("$3,500") mustEqual "String"
            // DataTypeParser.inferDataType("301 BRACKEN") mustEqual "String"
        }
    }

    val testList1 = List("the quick brown fox", "1234", "-123.456", "June 22, 2014", "")
    s"""DataTypeParser inferDataTypes("$testList1")""" should {
        "return List(String, Integer, Float, Date, UNKNOWN)" in {
        	val valTypes = DataTypeParser.inferDataTypes(testList1)
            valTypes must contain("String", "Integer", "Float", "Date", "UNKNOWN") 
        }
    }

    val testList2 = List("1953-03-05 02:00")
    s"""DataTypeParser inferDataTypes("$testList2")""" should {
        "return List(DateTime)" in {
            val valTypes = DataTypeParser.inferDataTypes(testList2)
            valTypes must contain("DateTime") 
        }
    }

    val testList3 = List("12:30")
    s"""DataTypeParser inferDataTypes("$testList3")""" should {
        "return List(Time)" in {
            val valTypes = DataTypeParser.inferDataTypes(testList3)
            valTypes must contain("Time") 
        }
    }

    val freebaseDb = "src/test/resources/datasets/freebase/music-artist-sample"
    s"""DataTypeParser inferDataTypes() on Freebase/DBPedia""" should {
        "detect the types Date,String,UNKNOWN for date_of_birth@freebase,description@freebase,active_end@freebase respectively" in {
            val ds1 = FreebaseDataLoader.load(freebaseDb)

            val inferredTypes = ds1.children.get.map({ case Attribute(id,_,values,_) => {
                    DataTypeParser.inferDataTypes(values).groupBy(identity).map({case (key,values) => (id,key,values.size)})
                }
            })

            inferredTypes.find({case attrList => attrList.head._1 == "date_of_birth@freebase"}).get.maxBy(_._3)._2 mustEqual "Date"
            inferredTypes.find({case attrList => attrList.head._1 == "description@freebase"}).get.maxBy(_._3)._2 mustEqual "String"
            inferredTypes.find({case attrList => attrList.head._1 == "active_end@freebase"}).get.maxBy(_._3)._2 mustEqual "UNKNOWN"
            
        }
    }    
}