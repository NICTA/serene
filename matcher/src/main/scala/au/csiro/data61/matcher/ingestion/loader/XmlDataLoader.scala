package au.csiro.data61.matcher.ingestion.loader

import java.io.File

import scala.io.Source

import au.csiro.data61.matcher.data.Metadata
import au.csiro.data61.matcher.data.DataModel
import au.csiro.data61.matcher.data.Attribute

import scala.xml._

case class XmlDataLoader(val datasetId: String,
                         val datasetName: String, 
                         val datasetDescription: String,
                         val excludeElements: Option[Set[String]] = None) extends FileLoaderTrait[DataModel] {
  
    def load(path: String): DataModel = {
        val xmlDataList = new File(path).listFiles.map(XML.loadFile)
        val instanceAttrValues = xmlDataList.map({parseXml(_,datasetId)})

        val allAttributeNamesSet = instanceAttrValues.flatMap({_.map({_._1})}).distinct.toSet
        val attrsWithNulls = instanceAttrValues.map({case attrsList => {
            allAttributeNamesSet.toSeq.map({case attrname => {
                    (attrname, attrsList.find({_._1 == attrname}).getOrElse(("",""))._2)
                }
            })
        }})

        val attrNameRegex = "^([^@]+)@.+".r
        val attrGroups = attrsWithNulls.zipWithIndex.flatMap({
            case (attrs,index) => attrs.map({
                case (attrname, attrval) => (index, attrname, attrval)
            })
        }).groupBy(_._2).map({case (grpId, grpVals) => (grpId, grpVals.map({case (idx,name,value) => (idx,value)}).toList)})
        lazy val dataset = new DataModel(datasetId,
                                         Some(Metadata(datasetName,datasetDescription)),
                                         None, Some(attributes))
        lazy val attributes: List[Attribute] = attrGroups
            .filter({
                case (id, _) => id match { 
                    case attrNameRegex(name) => excludeElements.map({!_.contains(name)}).getOrElse(true)
                    case _ => true
                }
            }).map({
                case (attrId: String, attrvals: List[(Int,String)]) => {
                    val attrname = attrId match {
                        case attrNameRegex(name) => name
                        case _ => "ATTRIBUTE_HAS_NO_NAME" 
                    }
                    Attribute(s"$attrId", 
                          Some(Metadata(attrname, s"$datasetName attribute $attrId")), 
                          attrvals.sortWith((attr1,attr2) => attr1._1 < attr2._1).map({_._2}),
                          Some(dataset))
                }
            }).toList

        dataset
    }
    
    def parseXml(node: Node, path: String = ""): List[(String,String)] = {
        val newPath = if(path.length>0) node.label+"@"+path else node.label

        val children = node.child
        if(children.size == 1 && children.head.label == "#PCDATA") {
            List((newPath, children.head.text))
        } else
        if(children.size > 0) {
            children.flatMap({parseXml(_,newPath)}).toList
        } else
        if(children.size == 0 && node.label != "#PCDATA") {
            List((newPath, node.text))
        } else {
            List()
        }
    }
}