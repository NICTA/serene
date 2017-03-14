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
package au.csiro.data61.types

import java.nio.file.Path

import au.csiro.data61.types.ColumnTypes.ColumnID
import au.csiro.data61.types.DataSetTypes.DataSetID
import au.csiro.data61.types.Exceptions.TypeException
import au.csiro.data61.types.GraphTypes._
import au.csiro.data61.types.ModelTypes.ModelID
import au.csiro.data61.types.SsdTypes.OwlDocumentFormat.OwlDocumentFormat
import au.csiro.data61.types.SsdTypes.{AttrID, SsdID}
import com.typesafe.scalalogging.LazyLogging
import edu.isi.karma.modeling.alignment.{SemanticModel => KarmaSSD}
import edu.isi.karma.modeling.ontology.OntologyManager
import org.joda.time.DateTime
import org.json4s.{DefaultFormats, _}

import scala.language.postfixOps


object SsdTypes {
  type SsdID = Int // id of the SSD
  type AttrID = Int        // id for the transformed column
  type OwlID = Int         // id of the owl
  type OctopusID = Int     // id for the alignment model

  /**
    * Octopus is the data structure which encapsulates models for Schema Matcher and Semantic Modeler.
    * It contains configuration parameters to initialize training for Schema Matcher and Semantic Modeler.
    *
    * @param id The ID key for the alignment
    * @param name User provided string as a shortcut to this octopus
    * @param ontologies The ontologies used for the alignment
    * @param ssds The list of SSDs for the construction of the alignment graph
    * @param lobsterID Id of the associated schema matcher model
    * @param modelingProps Modeling properties for semantic modeler; optional case class
    * @param semanticTypeMap Mapping of matcher:labels to URIs.
    * @param state State of Octopus
    * @param dateCreated Date of creation
    * @param dateModified Date of latests modification
    * @param description Description of the model
    */
  case class Octopus(id: OctopusID,
                     name: String,
                     ontologies: List[Int], // WARNING: Int should be OwlID! Json4s bug.
                     ssds: List[Int],       // WARNING: Int should be SsdID! Json4s bug.
                     lobsterID: ModelID,
                     modelingProps: Option[ModelingProperties],
                     semanticTypeMap: Map[String,String],
                     state: Training.TrainState,
                     dateCreated: DateTime,
                     dateModified: DateTime,
                     description: String
                    ) extends Identifiable[OctopusID]

  /**
    * OwlDocumentFormat is the format of the uploaded
    * document. This needs to be specified by the user
    * it is not auto-detected at this stage..
    */
  object OwlDocumentFormat extends Enumeration {
    type OwlDocumentFormat = Value

    val Turtle = Value("ttl")
    val Notation3 = Value("n3")
    val Rdf = Value("rdf")
    val Xml = Value("xml")
    val DefaultOwl = Value("owl")
  }

  /**
    * Owl is a reference to the Owl file storage.
    *
    * @param id The ID key for the OWL file storage.
    * @param name The name of the original uploaded OWL file.
    * @param format The format of the OWL file.
    * @param description The description of the OWL file.
    * @param dateCreated  The creation time.
    * @param dateModified The last modified time.
    */
  case class Owl(
    id: OwlID,
    name: String,
    format: OwlDocumentFormat,
    description: String,
    dateCreated: DateTime,
    dateModified: DateTime) extends Identifiable[OwlID]

}

/**
  * This is the class to represent the semantic source description (SSD).
  * We need a JON serializer and general interpreter of SSD.
  * SSD needs to be split apart to initialize data structures as indicated in the schema matcher api.
  * SSD needs also to be split apart to initialize data structures for the Karma tool.
  * NOTE: for now we will automatically generate attributes as identical to columns
  *
  * @param id ID of the dataset
  * @param name Name of the dataset
  * @param attributes List of attributes = transformed columns
  * @param ontology List of location path strings of ontologies
  * @param semanticModel Semantic Model of the data source; optional
  * @param mappings Mappings between attribute ids of the data source and node ids in the semantic model; optional
  * @param dateCreated Date when it was created
  * @param dateModified Date when it was last modified
  */
case class Ssd(id: SsdID,
               name: String,
               attributes: List[SsdAttribute],
               ontology: List[Int], // Int=OwlID ==> we have to use Int due to JSON bug
               semanticModel: Option[SemanticModel],
               mappings: Option[SsdMapping],
               dateCreated: DateTime,
               dateModified: DateTime
              ) extends Identifiable[SsdID] with LazyLogging {
  /**
    * we need to check consistency of SSD
    * -- semanticModel
    * -- mappings: from existing attribute to existing node
    */
  def isConsistent: Boolean = {
    // attributes contain columnIds which are available among columns -- we need to check this at the interface level now!
//    val attrCheck =
//    attributes.forall { attr =>
//      attr.columnIds
//        .forall(
//          columns.map(_.id).contains
//        )
//    }
    // mappings refer to attributeIDs which are available among attributes
    val attrIdCheck: Boolean = mappings match {
      case Some(maps) => maps.mappings.keys
        .forall(attributes.map(_.id).contains)
      case None => true
    }

    // mappings refer to nodeIDs which exist in the semantic model
    val nodeIdCheck: Boolean = mappings match {
      case Some(maps) => maps.mappings.values
        .forall(getSMNodeIds.contains)
      case None => true
    }
      // we map to distinct nodes in the semantic model
    val distinctCheck = mappings match {
      case Some(maps) => maps.mappings.values.toList.distinct.size == maps.mappings.values.size
      case None => true
    }

    attrIdCheck && nodeIdCheck && distinctCheck
  }

  /**
    * mappings are not empty
    * semantic model is a connected graph
    */
  def isComplete: Boolean = {
    // Semantic types need to be checked in ontology. This check is now done at the UI level and inside Karma code.
    // Here we check that the semantic model is not empty and is a connected graph.
    val semModelCheck = semanticModel match {
      case Some(sm) =>
        sm.getNodes.nonEmpty && sm.isConnected
      case None =>
        false
    }

    // mappings are not empty
    val mappingCheck = mappings match {
      case Some(maps) =>
        maps.mappings.nonEmpty
      case None =>
        false
    }
    // ssd is consistent and above two checks
    isConsistent && semModelCheck && mappingCheck
  }

  /**
    * Helper function to obtain list of node ids in the semantic model.
    * It returns an empty list if the semantic model is None.
    *
    * @return
    */
  private def getSMNodeIds: List[NodeID] = {
    semanticModel match {
      case Some(sm) =>
        sm.getNodes.map(_.id)
      case None =>
        List()
    }
  }

  def notMappedAttributes: List[SsdAttribute] = {
    mappings match {
        // in case some attributes are mapped -> get the set difference of two lists
      case Some(maps) =>
        val mappedAttributes = maps.mappings.keys.toSet
        attributes.filterNot(attr => mappedAttributes.contains(attr.id))
        // in case no attributes are mapped -> get all attribute ids
      case None =>
        attributes
    }
  }

  /**
    * Update the semantic model and the mappings of the current semantic source description.
    * This method affects only the semantic model, the mappings and dateModified.
    *
    * @param karmaSM Semantic Model returned from the Karma tool, this is type KarmaGraph.
    * @return
    */
  def update(karmaSM: KarmaGraph): Ssd = {
    logger.info(s"Updating SSD $id")
    this.copy(semanticModel = Some(karmaSM.toSemanticModel),
      mappings = Some(SsdMapping(karmaSM.columnNodeMappings)),
      dateModified = DateTime.now
    )
  }

  /**
    * Convert Semantic Source Description to the Karma Semantic Model data structure.
    * We need the Karma ontology manager for this purpose.
    * The ontology manager is initialized in the KarmaAPI.
 *
    * @param ontoManager Karma ontology manager
    * @return
    */
  def toKarmaSemanticModel(ontoManager: OntologyManager): Option[KarmaSemanticModel] = {
    // we need only to provide proper conversion of Karma graph
    (semanticModel, mappings) match {
      case (Some(sm), Some(maps)) =>
        val karmaGraph = sm.toKarmaGraph(maps, ontoManager)
        val karmaSSD = new KarmaSSD(id.toString, karmaGraph.graph)
        karmaSSD.setName(name)
        Some(KarmaSemanticModel(karmaSSD))
      case _ => None
    }
  }
}


/**
  * Column specification as indicated in .ssd files.
  * NOTE: different from Column in schema-matcher since we need it for proper JSON serialization of SSD.
 *
  * @param id Column ID
  * @param name Name of the column
  */
case class SsdColumn(id: ColumnID,
                     name: String)

object SsdColumn {
  /**
    * Create default SSDColumn based on ColumnID.
    *
    * @param id id of the column
    * @return SSDColumn
    */
  def apply(id: ColumnID): SsdColumn = {
    SsdColumn(id, "") // not implemented really
  }
}

/**
  * Specification for a transformed column (attribute) as indicated in .ssd files
 *
  * @param id ID of the attribute
  * @param name Name of the attribute
  * @param label Label of the transformation
  * @param columnIds List of IDs of source columns
  * @param sql String which specifies SQL for the transformation
  */
case class SsdAttribute(id: AttrID,
                        name: String,
                        label: String,
                        columnIds: List[Int], // bug in json serializer
                        sql: String)

object SsdAttribute {
  /**
    * Create default identity attribute for a column.
    *
    * @param column SSDColumn
    * @param id id of the attribute
    * @param tableName name of the table
    * @return SSDAttribute
    */
  def apply(column: SsdColumn, id: AttrID, tableName: String): SsdAttribute = {
    SsdAttribute(id, column.name, "identity",
      List(column.id), s"select ${column.name} from $tableName")
  }

  /**
    * Create default identity attribute for a column.
    * We do not have support for transformations currently -- left for future implementation.
    *
    * @param id id of the column
    * @return SSDAttribute
    */
  def apply(id: ColumnID): SsdAttribute = SsdAttribute(id, "", "identity", List(id), "not implemented")

  /**
    * Create default identity attribute for a column.
    * We do not have support for transformations currently -- left for future implementation.
    *
    * @param id id of the column
    * @param name name of the column
    * @return SSDAttribute
    */
  def apply(id: ColumnID, name: String): SsdAttribute = SsdAttribute(id, name, "identity", List(id), "not implemented")
}

case class SsdMapping(mappings: Map[Int,Int]) {
  def getAttributeIds: List[AttrID] = mappings.keys.toList
}

/**
  * custom JSON serializer for the SSDMapping
  */
case object SsdMappingSerializer extends CustomSerializer[SsdMapping](
  format => (
    {
      case jv: JValue =>
        implicit val formats = DefaultFormats
        val tuples = jv.extract[List[Map[String,Int]]]

        // there should be only attribute and node as keys
        if(tuples.nonEmpty && tuples.flatMap(_.keys).sorted.distinct != List("attribute", "node")){
          throw TypeException("Wrong ssd mappings!")
        }

        SsdMapping(
          tuples.map(_.toList.sorted).map { case List(aID,nID) =>
            (aID._2, nID._2)
          }.toMap
        )
    }, {
    case ssdMap: SsdMapping =>
      implicit val formats = DefaultFormats
      Extraction.decompose(
        ssdMap.mappings.toList
          .map {
            case (aID, nID) =>
              Map("attribute" -> aID, "node"-> nID)
          }
      )
    })
)


/**
  * Information about column which is needed for the semantic modeller
 *
  * @param id ID of the column
  * @param index Ordinal placement of column in the source
  * @param path Path string for the source
  * @param name Name of the column
  * @param datasetID ID of the data source
  * @param datasetName Name of the data source
  * @param semanticScores Map of scores for semantic classes
  */
case class ColumnDesc(id: ColumnID,
                      index: Int,
                      path: String, // this is not needed actually...
                      name: String,
                      datasetID: DataSetID,
                      datasetName: String,
                      semanticScores: Map[String, Double] // these are predictions of the schema matcher
                     )


/**
  * Configuration parameters for the semantic modeler
  * @param compatibleProperties
  * @param ontologyAlignment
  * @param addOntologyPaths
  * @param mappingBranchingFactor
  * @param numCandidateMappings
  * @param topkSteinerTrees
  * @param multipleSameProperty
  * @param confidenceWeight should be in range [0..1]
  * @param coherenceWeight should be in range [0..1]
  * @param sizeWeight should be in range [0..1]
  * @param numSemanticTypes
  * @param thingNode
  * @param nodeClosure
  * @param propertiesDirect
  * @param propertiesIndirect
  * @param propertiesSubclass
  * @param propertiesWithOnlyDomain
  * @param propertiesWithOnlyRange
  * @param propertiesWithoutDomainRange
  */
case class ModelingProperties(compatibleProperties: Boolean = true,
                              ontologyAlignment: Boolean = false,
                              addOntologyPaths: Boolean = false,
                              mappingBranchingFactor: Int = 50,
                              numCandidateMappings: Int = 10,
                              topkSteinerTrees: Int = 10,
                              multipleSameProperty: Boolean = false,
                              confidenceWeight: Double = 1.0,
                              coherenceWeight: Double = 1.0,
                              sizeWeight: Double = 1.0,
                              numSemanticTypes: Int = 4,
                              thingNode: Boolean = false,
                              nodeClosure: Boolean = true,
                              propertiesDirect: Boolean = true,
                              propertiesIndirect: Boolean = true,
                              propertiesSubclass: Boolean = true,
                              propertiesWithOnlyDomain: Boolean = true,
                              propertiesWithOnlyRange: Boolean = true,
                              propertiesWithoutDomainRange: Boolean = false
                             )
