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

package au.csiro.data61.core.storage

import java.io._
import java.nio.file.{Path, Paths}

import au.csiro.data61.types.SsdTypes.{Owl, OwlID, SsdID}
import au.csiro.data61.types._
import au.csiro.data61.core.Serene
import org.json4s.jackson.JsonMethods._

import scala.language.postfixOps

/**
  * Object for storing known semantic source descriptions.
  */
object SsdStorage extends Storage[SsdID, Ssd] {

  implicit val keyReader: Readable[SsdID] = Readable.ReadableInt

  def rootDir: String = new File(Serene.config.storageDirs.ssd).getAbsolutePath

  def extract(stream: FileInputStream): Ssd = {
    parse(stream).extract[Ssd]
  }

  /**
    * Returns the location of the .ssd metadata file for id
    *
    * @param id The ID for the semantic source desc
    * @return
    */
  override protected def getPath(id: SsdID): Path = {
    Paths.get(getDirectoryPath(id).toString, s"$id.ssd")
  }

  /**
    * Get the list of distinct location strings for ontologies.
    * @return List of location strings for ontologies used in ssd.
    */
  def getOntologies(id: SsdID): List[String] = {
    //    get(id).flatMap(_.ontology).toList.map(OwlStorage.get).flatMap {
    //      case Some(o: Owl) => Some(o.path.toString)
    //      case _ => None
    //    }
    List("dummy_path_here")
    // TODO: get paths from OwlStorage
  }

  /**
    * Check if Ssd is consistent.
    *
    * @param ssd Semantic Source Description for which we check consistency
    * @return
    */
  def isConsistent(ssd: Ssd): Boolean = {
    logger.info(s"Checking consistency of Ssd ${ssd.id}")

    // make sure that referenced columns exist in DataSetStorage
    val columnCheck = ssd.attributes
      .forall(attr => DatasetStorage.columnMap.keySet.contains(attr.id))
    // make sure that referenced ontologies exist in OwlStorage
    val owlCheck = ssd.ontologies.forall(OwlStorage.keys.toSet.contains)
    // make sure the semantic model is complete
    val isComplete = ssd.isComplete

    // TODO: do we need to check dates here?
    // make sure the references (datasets and ontologies) are older than the SSD
    // allBefore = refDates.forall(_.isBefore(ssd.dateModified))

    isComplete && owlCheck && isComplete

  }

}
