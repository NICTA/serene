/**
  * Copyright (C) 2015-2016 Data61, Commonwealth Scientific and Industrial Research Organisation (CSIRO).
  * See the LICENCE.txt file distributed with this work for additional
  * information regarding copyright ownership.
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package au.csiro.data61.matcher.data

abstract class AbstractDataModel {
  def id: String
  def metadata: Option[Metadata]

  override def toString(): String = {
    val meta = metadata.getOrElse(Metadata("",""))
    s"""{id: "$id", metadata: ${meta.toString}}"""
  }
}

trait HasChildren {
  def children: Option[List[AbstractDataModel]]
}

trait HasParent {
  def parent: Option[AbstractDataModel]
}


class DataModel(val id: String,
                val metadata: Option[Metadata],
                parentf: => Option[AbstractDataModel],
                childrenf: => Option[List[AbstractDataModel]]) extends AbstractDataModel
  with HasChildren
  with HasParent
  with Serializable {

  lazy val parent = parentf
  lazy val children = childrenf

  override def toString(): String = {
    val childrenStr = children.getOrElse(List()).map({_.toString}).mkString(",")
    val meta = metadata.getOrElse(Metadata("",""))
    s"""{id: "$id", metadata: ${meta.toString}, children: [${childrenStr}]}"""
  }
}

object DataModel {
  def getAllAttributes(dataModel: DataModel): List[Attribute] = {
    dataModel.children match {
      case Some(children: List[AbstractDataModel]) => children.flatMap({
        case child: DataModel => getAllAttributes(child)
        case child: Attribute => List(child)
      })
      case None => List()
    }
  }

  def copy(datamodel: DataModel, parent: Option[DataModel], attrsToIncl: Set[String]): DataModel = {
    lazy val dmCopy: DataModel = new DataModel(datamodel.id, datamodel.metadata, parent, Some(childrenCopy))
    lazy val childrenCopy: List[AbstractDataModel] = copy(datamodel.children.get, dmCopy, attrsToIncl)
    dmCopy
  }

  def copy(datamodels: List[AbstractDataModel], parent: AbstractDataModel, attrsToIncl: Set[String]): List[AbstractDataModel] = {
    datamodels.filter({
      case attr: Attribute => attrsToIncl contains attr.id
      case datamodel: DataModel => true
    }).map({
      case dm: DataModel => {
        lazy val dmCopy: DataModel = new DataModel(dm.id, dm.metadata, Some(parent), Some(childrenCopy))
        lazy val childrenCopy: List[AbstractDataModel] = copy(dm.children.get, dmCopy, attrsToIncl)
        dmCopy
      }
      case attr: Attribute => Attribute(attr.id,attr.metadata,attr.values,Some(parent))
    })
  }
}

class Attribute(val id: String,
                val metadata: Option[Metadata],
                val values: List[String],
                parentf: => Some[AbstractDataModel])
  extends AbstractDataModel
    with HasParent
    with Serializable {
  lazy val parent = parentf

  override def toString(): String = {
    val meta = metadata.getOrElse(Metadata("",""))
    s"""{id: "$id", metadata: ${meta.toString}, values: ${values.take(5).mkString(",")}}"""
  }
}

object Attribute {
  def apply(id: String,
            metadata: Option[Metadata],
            values: List[String],
            parentf: => Some[AbstractDataModel]) = new Attribute(id,metadata,values,parentf)
  def unapply(a: Attribute) = Some(a.id, a.metadata, a.values, a.parent)
}

case class PreprocessedAttribute(val rawAttribute: Attribute, val preprocessedDataMap: Map[String,Any])
