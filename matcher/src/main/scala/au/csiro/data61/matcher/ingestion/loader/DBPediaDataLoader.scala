package au.csiro.data61.matcher.ingestion.loader

import java.io.File

import scala.io.Source

import au.csiro.data61.matcher.data.Metadata
import au.csiro.data61.matcher.data.DataModel
import au.csiro.data61.matcher.data.Attribute

import org.json4s._
import org.json4s.native.JsonMethods._

object DBPediaDataLoader extends FileLoaderTrait[DataModel] {
    def databaseId = "dbpedia"
    type AttributeExtractorPair = (String,(JValue) => String)
    type InstanceAttributes = List[(String,String)]

    def attributeExtractors = List[AttributeExtractorPair](
        ("abstract", ((json: JValue) => extractEnglishValue(json \\ "http://dbpedia.org/ontology/abstract"))),
        ("givenName", ((json: JValue) => extractEnglishValue(json \\ "http://xmlns.com/foaf/0.1/givenName"))),
        ("origin", ((json: JValue) => extractHeadValue(json \\ "http://dbpedia.org/property/origin"))),
        ("activeYearsEndYear", ((json: JValue) => extractHeadValue(json \\ "http://dbpedia.org/ontology/activeYearsEndYear"))),
        ("dbpedia_id", ((json: JValue) => extractHeadValue(json \\ "http://dbpedia.org/ontology/wikiPageID"))),
        ("birthDate", ((json: JValue) => extractHeadValue(json \\ "http://dbpedia.org/property/birthDate"))),
        ("placeOfBirth", ((json: JValue) => extractHeadValue(json \\ "http://dbpedia.org/property/birthPlace"))),
        ("yearsActive", ((json: JValue) => extractHeadValue(json \\ "http://dbpedia.org/property/yearsActive"))),
        ("website", ((json: JValue) => extractHeadValue(json \\ "http://dbpedia.org/property/website"))),
        ("occupation", ((json: JValue) => extractHeadValue(json \\ "http://dbpedia.org/ontology/occupation"))),
        ("surname", ((json: JValue) => extractHeadValue(json \\ "http://xmlns.com/foaf/0.1/surname"))),
        ("sameAs", ((json:JValue)) => extractValueWithSubstring(json \\ "http://www.w3.org/2002/07/owl#sameAs", "freebase"))
    )

    def extractHeadValue(jsonObj: JValue): String =  jsonObj match {
        case JArray(values) => if(values.size > 0) {
            (values.head \ "value") match {
                case JString(value) => value
                case JInt(value) => value.toString
                case _ => "NOT JSTRING"
            }
        } else {
            "JARRAY HAS NO ELEMENTS"
        }
        case x => ""
    } 

    def extractEnglishValue(jsonObj: JValue): String =  {
        jsonObj match {
            case JArray(values) => if(values.size > 0) {
                val englishAttr = values.find({
                    case obj: JObject => (obj \ "lang") match {
                        case JString(value) => value == "en"
                        case _ => false
                    }
                    case _ => false 
                })
                englishAttr match {
                    case Some(a) => (a \ "value") match {
                        case JString(value) => value
                        case _ => ""
                    }
                    case None => "NO LANG==EN"
                }
            } else {
                "EMPTY ARRAY"
            }
            case _ => ""
        }
    } 

    def extractValueWithSubstring(jsonObj: JValue, substring: String): String = {
        jsonObj match {
            case JObject(values) => if(values.size > 0) {
                values.flatMap({case JField(name,obj) => obj match {
                        case JArray(sameAsList) => sameAsList.map({case x => (x \\ "value") match {case JString(url) => url}})
                    }
                }).find({_.indexOf(substring) >= 0}).getOrElse("NOT FOUND")
            } else {
                "EMPTY ARRAY"
            }
            case _ => ""
        }
    }

    def load(path: String): DataModel = {
        val instances: List[InstanceAttributes] = (new File(path)).listFiles.map({
            case f => {
                val json = parse(Source.fromFile(f).getLines.foldLeft("")(_+"\n"+_))
                attributeExtractors.map({
                    case (attrname, extractor) => (attrname, extractor(json))
                })
            }
        }).toList

        val attrGroups = instances.zipWithIndex.flatMap({
            case (attrs,index) => attrs.map({
                case (attrname, attrval) => (index, attrname, attrval)
            })
        }).groupBy(_._2).map({case (grpId, grpVals) => (grpId, grpVals.map({case (idx,name,value) => (idx,value)}))})
        lazy val dataset = new DataModel("DBPediaDataSet",
                                         Some(Metadata("DBPediaDataSet","DBPedia DataSet")),
                                         None, Some(attributes))
        lazy val attributes: List[Attribute] = attrGroups.map({
            case (attrname: String, attrvals: List[(Int,String)]) => {
                Attribute(s"$attrname@$databaseId", 
                      Some(Metadata(attrname, s"DBPedia attribute $attrname")), 
                      attrvals.sortWith((attr1,attr2) => attr1._1 < attr2._1).map({_._2}),
                      Some(dataset))
            }
        }).toList

        dataset
    }
}