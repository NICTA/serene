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
package au.csiro.data61.core.drivers

import java.io.InputStream

import au.csiro.data61.core.api.{BadRequestException, InternalException, NotFoundException}
import au.csiro.data61.core.drivers.Generic._
import au.csiro.data61.core.storage.{ModelStorage, OctopusStorage, OwlStorage, SsdStorage}
import au.csiro.data61.types.ColumnTypes._
import au.csiro.data61.types.ModelTypes._
import au.csiro.data61.types.SsdTypes.OwlDocumentFormat._
import au.csiro.data61.types.SsdTypes._
import com.twitter.io.Reader
import com.typesafe.scalalogging.LazyLogging
import org.joda.time.DateTime

import scala.util.Try

object OwlInterface extends StorageInterface[OwlKey, Owl] with LazyLogging {

  override val storage = OwlStorage

  protected def missingReferences(resource: Owl): StorageDependencyMap = {
    // owl does not have references to be checked
    StorageDependencyMap()
  }

  protected def dependents(resource: Owl): StorageDependencyMap = {
    // SSDs which refer to this ontology
    val ssdRefIds: List[SsdID] = SsdStorage.keys
      .flatMap(SsdStorage.get)
      .map(x => (x.id, x.ontologies.toSet))
      .filter(_._2.contains(resource.id))
      .map(_._1)

    // octopi which directly refer to this ontology
    val octoRefIds1: List[OctopusID] = OctopusStorage.keys
      .flatMap(OctopusStorage.get)
      .map(x => (x.id, x.ontologies.toSet))
      .filter(_._2.contains(resource.id))
      .map(_._1)

    // octopi which refer to ssdRefIds
    val octoRefIds2: List[OctopusID] = OctopusStorage.keys
      .flatMap(OctopusStorage.get)
      .map(x => (x.id, x.ssds.toSet))
      .filter {
        case (octoID, ssds) =>
          (ssdRefIds.toSet & ssds).nonEmpty
      }
      .map(_._1)

    StorageDependencyMap(
      ssd = ssdRefIds,
      octopus = (octoRefIds1 ++ octoRefIds2).distinct
    )
  }

  /**
    * Creates an OWL document with related information.
    *
    * @param description The description of the OWL document.
    * @param format The format of the OWL document.
    * @param inputStream The input stream of the OWL document.
    * @return The created OWL information if successful. Otherwise the exception that caused the
    *         failure.
    */
  def createOwl(name: String,
                description: String,
                format: OwlDocumentFormat,
                inputStream: InputStream): Option[Owl] = {
    val id = genID
    val now = DateTime.now
    val owl = Owl(
      id = id,
      name = name,
      format = format,
      description = description,
      dateCreated = now,
      dateModified = now
    )

    for {
      owlId <- add(owl)
      _ <- OwlStorage.writeOwlDocument(id, inputStream)
      curOwl <- get(owlId)
    } yield curOwl
  }


  /**
    * Gets the original OWL file uploaded to the server.
    *
    * @param owl The owl object
    * @return A buffered reader object
    */
  def getOwlDocument(owl: Owl): Try[Reader] = Try {
    Reader.fromFile(OwlStorage.getOwlDocumentPath(owl.id).map(_.toFile).get)
  }

  /**
    * Updates information about an OWL document.
    *
    * @param id The ID of the OWL document.
    * @param description The description of the OWL document.
    * @return Updated information of the OWL document if successful. Otherwise the exception that
    *         caused the failure.
    */
  def updateOwl(id: OwlID,
                description: Option[String],
                filename: Option[String],
                stream: Option[InputStream]
               ): Try[Owl] = Try {

    OwlStorage.get(id) match {
      case Some(owl) =>
        synchronized {
          // first we just try to write the new document...
          stream.foreach(OwlStorage.writeOwlDocument(id, _))

          // now we update the description
          val updatedOwl = owl.copy(
            name = filename.getOrElse(owl.description),
            description = description.getOrElse(owl.description),
            dateModified = DateTime.now())

          update(updatedOwl)
            .flatMap(get)
            .getOrElse(throw InternalException(s"Failed to update Owl at $id"))
        }

      case None =>
        throw BadRequestException(s"Owl $id not found.")
    }
  }

}
