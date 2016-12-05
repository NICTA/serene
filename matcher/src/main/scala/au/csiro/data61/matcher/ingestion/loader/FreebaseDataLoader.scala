package au.csiro.data61.matcher.ingestion.loader

import java.io.File

import scala.io.Source

import au.csiro.data61.matcher.data.Metadata
import au.csiro.data61.matcher.data.DataModel
import au.csiro.data61.matcher.data.Attribute

import org.json4s._
import org.json4s.native.JsonMethods._

object FreebaseDataLoader extends FileLoaderTrait[DataModel] {
    def databaseId = "freebase"
    type AttributeExtractorPair = (String,(JObject) => String)
    type InstanceAttributes = List[(String,String)]

    def attributeExtractors = List[AttributeExtractorPair](
        ("freebase_id", ((json: JObject) => getHeadValue(json, "id"))),
        ("freebase_mid", ((json: JObject) => getConcatenatedValues(json, "mid"))),
        ("name", ((json: JObject) => getUniqueValue(json, "name"))),
        ("type", ((json: JObject) => getUniqueValue(json, "type"))),
        ("origin", ((json: JObject) => getHeadValue(json, "/music/artist/origin", "name"))),
        ("active_start", ((json: JObject) => getHeadValue(json, "/music/artist/active_start"))),
        ("active_end", ((json: JObject) => getHeadValue(json, "/music/artist/active_end"))),        
        ("description", ((json: JObject) => getHeadValue(json, "/common/topic/description"))),
        ("nationality", ((json: JObject) => getHeadValue(json, "/people/person/nationality"))),
        ("height_meters", ((json: JObject) => getUniqueValue(json, "/people/person/height_meters"))),
        ("profession", ((json: JObject) => getHeadValue(json, "/people/person/profession"))),
        ("gender", ((json: JObject) => getHeadValue(json, "/people/person/gender"))),
        ("date_of_birth", ((json: JObject) => getHeadValue(json, "/people/person/date_of_birth"))),
        ("age", ((json: JObject) => getUniqueValue(json, "/people/person/age")))
    )
  
    def getUniqueValue(json: JValue, propName: String): String = {
      (json \ propName) match {
        case JString(value) => value
        case _ => ""
      }
    }

    def getHeadValue(json: JValue, propName: String, valueName: String = "value"): String = {
      (json \ propName) match {
          case JArray(values) => if(values.size > 0) {
                  (values.head \ valueName) match {
                    case JString(value) => value
                    case _ => ""
                  }
              } else {
                  ""
              }
          case _ => ""
      } 
    }

    def getConcatenatedValues(json: JValue, propName: String): String = {
      (json \ propName) match {
          case JArray(values) => if(values.size > 0) {
                  (values.head \ "value") match {
                    case JArray(mids) => mids.map({
                      case JString(value) => value
                      case _ => ""
                    }).mkString(",")
                    case _ => ""
                  }
              } else {
                  ""
              }
          case _ => ""
      }
    }

    def load(path: String): DataModel = {
        val instances: List[InstanceAttributes] = (new File(path)).listFiles.flatMap({
            case f => {
                val json = parse(Source.fromFile(f).getLines.foldLeft("")(_+"\n"+_))
                val rows = (json \\ "result") match {
                    case JArray(list) => list
                    case _ => ""
                } 

                rows.asInstanceOf[List[JObject]].map({
                    case row: JObject => {
                        attributeExtractors.map({
                            case (attrname, extractor) => (attrname, extractor(row))
                        })
                    }
                })
            }
        }).toList

        val attrGroups = instances.zipWithIndex.flatMap({
            case (attrs,index) => attrs.map({
                case (attrname, attrval) => (index, attrname, attrval)
            })
        }).groupBy(_._2).map({case (grpId, grpVals) => (grpId, grpVals.map({case (idx,name,value) => (idx,value)}))})
        lazy val dataset = new DataModel("FreebaseDataSet",
                                         Some(Metadata("FreebaseDataSet","Freebase DataSet")),
                                         None, Some(attributes))
        lazy val attributes: List[Attribute] = attrGroups.map({
            case (attrname: String, attrvals: List[(Int,String)]) => {
                Attribute(s"$attrname@$databaseId", 
                      Some(Metadata(attrname, s"Freebase attribute $attrname")), 
                      attrvals.sortWith((attr1,attr2) => attr1._1 < attr2._1).map({_._2}),
                      Some(dataset))
            }
        }).toList

        dataset
    }
}