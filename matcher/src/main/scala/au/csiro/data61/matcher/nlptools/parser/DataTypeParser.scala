package au.csiro.data61.matcher.nlptools.parser

import com.joestelmach.natty._
import java.util.Calendar
import au.csiro.data61.matcher.data._

import com.mdimension.jchronic._
import com.mdimension.jchronic.tags._
import com.mdimension.jchronic.utils._

import scala.collection.JavaConverters._

object DataTypeParser {
    private val floatRegex = """^[+-]?[0-9]*\.[0-9]+""".r
    private val intRegex = """^[+-]?[0-9]+""".r
    private val booleanRegex = """^([01]{1})|([TF]{1})|([tf]{1})|([YN]{1})|([yn]{1})""".r
    private val timeOnlyRegex = """([0-9]{1,2}:[0-9]{1,2}(:[0-9]{1,2}(\.[0-9]+)?)?[ ]*(am|AM|pm|PM)?)|([0-9]{1,2}(:?[0-9]{1,2}(:[0-9]{1,2}(\.[0-9]+)?)?)?[ ]*(am|AM|pm|PM))"""
    private val hasTimeRegex1 = s"""([^:]+) (${timeOnlyRegex})(.*)""".r
    private val hasTimeRegex2 = s"""(.*)(${timeOnlyRegex})([^:]*)""".r

    def inferDataTypes(vals: List[String]): List[String] = vals.map(inferDataType)

    def inferDataType(value: String): String = {
        //match most specific data types first
        if(value.trim.length == 0) "UNKNOWN"
        else if(floatRegex.pattern.matcher(value).matches) "Float"
        else if(booleanRegex.pattern.matcher(value).matches) "Boolean"
        else if(intRegex.pattern.matcher(value).matches) {
            try {
                val v = value.toInt
                val currYear = Calendar.getInstance.get(Calendar.YEAR)
                val maxFutureYears = 500
                if(value.length == 4 && v >= 1500 && v <= currYear+maxFutureYears) {
                    "Date"
                } else {
                    "Integer"
                }
            } catch {
                case e: NumberFormatException =>
                    //try to parse as long
                    try {
                        val v = value.toLong
                        "Long"
                    } catch {
                        case e: Exception => "String"
                    }
                case e: Exception => "String"
            }
        }
        else if(timeOnlyRegex.r.pattern.matcher(value).matches) "Time"
        else if(hasTime(value) && isDate(value)) "DateTime"
        else if(isDate(value))  "Date"
        else "String"
    }

    def hasTime(s: String): Boolean = {
        hasTimeRegex1.pattern.matcher(s).matches || hasTimeRegex2.pattern.matcher(s).matches
    }

    def isDate(s: String): Boolean = {
        s match {
            case hasTimeRegex1(groups@_*) => {
                !DateParser.parse(groups.head).isEmpty
            }
            case hasTimeRegex2(groups@_*) => {
                !DateParser.parse(groups.last.trim).isEmpty
            }
            case _ => {
                !DateParser.parse(s).isEmpty
            }
        }
    }
}

object DateParser {
    val nattyParser = new Parser()

    def parse(datestr: String): Option[Date] = {
        // val nattyResult = parseWithNatty(datestr)
        // if(!nattyResult.isEmpty) {
        //     nattyResult
        // } else {
            parseWithJChronic(datestr)
        // }
    }

    //NOTE: Natty gives too much false positives!
    def parseWithNatty(datestr: String): Option[Date] = {
        if(datestr == "" || datestr.trim.length == 0) None
        else {
            try {
                val dateGroups = nattyParser.parse(datestr)
                if(dateGroups == null || dateGroups.size == 0) {
                    None
                } else {
                    val date = dateGroups.asScala.head.getDates.asScala.head
                    var calendar = Calendar.getInstance()
                    calendar.setTime(date)
                    Some(Date(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH), calendar.get(Calendar.DAY_OF_MONTH)))
                }
            } catch {
                case e: RuntimeException => {
                    //Chronic seems to be unstable for general text input
                    None
                }
            }
        }
    }

    def parseWithJChronic(datestr: String): Option[Date] = {
        if(datestr == "" || datestr.trim.length == 0) None
        else {
            try {
                val parseResult = Chronic.parse(datestr, new Options(false))
                if(parseResult == null) {
                    None
                } else {
                    val calendar = parseResult.getBeginCalendar
                    Some(Date(calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH), calendar.get(Calendar.DAY_OF_MONTH)))
                }
            } catch {
                case e: RuntimeException => {
                    //Chronic seems to be unstable for general text input
                    None
                }
            }
        }
    }
}