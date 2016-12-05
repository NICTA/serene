package au.csiro.data61.matcher.matcher.features

import org.json4s._
import org.json4s.native.JsonMethods._

import scala.io._
import com.typesafe.scalalogging.LazyLogging

object FeatureSettings extends LazyLogging {
    implicit val formats = DefaultFormats
    
    def load(f: String): FeatureSettings = {
        // use this method if there is no type map!!!
        val json = parse(Source.fromFile(f).getLines.mkString("\n"))
        val activeFeatures = parseSet(json, "activeFeatures")
        val activeGroupFeatures = parseSet(json, "activeFeatureGroups")
        val featureExtractorParams = parseFeatureExtractorParams(json, "featureExtractorParams")

        FeatureSettings(activeFeatures, activeGroupFeatures, featureExtractorParams, None)
    }

    def load(f: String, rootFilePath: String): FeatureSettings = {
        // use this method if there is a type map!!!
        parseJson(Source.fromFile(f).getLines.foldLeft("")(_+"\n"+_), rootFilePath)
    }

    def parseJson(jsonString: String, rootFilePath: String): FeatureSettings = {
        val jsonConfig = parse(jsonString)
        val activeFeatures = parseSet(jsonConfig, "activeFeatures")
        val activeGroupFeatures = parseSet(jsonConfig, "activeFeatureGroups")
        val featureExtractorParams = parseFeatureExtractorParams(jsonConfig, "featureExtractorParams")
        FeatureSettings(activeFeatures, activeGroupFeatures, featureExtractorParams, Some(rootFilePath))
    }

    def parseSet(json: JValue, xpath: String): Set[String] = {
        (json \\ xpath match {
            case JArray(l: List[JString]) => {
                l.map({_.extract[String]})
            }
        }).toSet
    }

    def parseFeatureExtractorParams(json: JValue, xpath: String): Map[String,Map[String,String]] = {
        val paramsList = (json \\ xpath).extract[List[Map[String,String]]]
        val temp: List[(String,Map[String,String])] = paramsList
          .map({case feParams => {
//              println(feParams)
//              println(feParams("name"))
              (feParams("name"), feParams)
          }
          })
        temp.toMap
    }
}

case class FeatureSettings(
    activeFeatures: Set[String], 
    activeGroupFeatures: Set[String], 
    featureExtractorParams: Map[String, Map[String,String]],
    rootFilePath: Option[String] //type map
 )