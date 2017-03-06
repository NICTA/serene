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

import au.csiro.data61.core.api.{NotFoundException, BadRequestException, InternalException}
import au.csiro.data61.core.drivers.Generic._
import au.csiro.data61.core.storage.OwlStorage
import au.csiro.data61.types.SsdTypes.OwlDocumentFormat._
import au.csiro.data61.types.SsdTypes.{Owl, OwlID}
import com.twitter.io.Reader
import com.typesafe.scalalogging.LazyLogging
import org.joda.time.DateTime

import scala.util.Try

object OwlInterface extends StorageInterface[OwlID, Owl] with LazyLogging {

  override val storage = OwlStorage

  protected def missingReferences[Owl](resource: Owl): StorageDependencyMap = Map()

  protected def dependents[Owl](resource: Owl): StorageDependencyMap = Map()

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
      owlId <- OwlStorage.add(id, owl)
      owlPath <- OwlStorage.writeOwlDocument(id, inputStream)
      curOwl <- OwlStorage.get(owlId)
    } yield curOwl

    //    OwlStorage.add(id, owl) match {
    //      case Some(_) =>
    //          OwlStorage.writeOwlDocument(id, inputStream)
    //      case None =>
    //        throw new IOException(s"Owl $id could not be created.")
    //    }
  }

  /**
    * Gets the IDs of available OWL documents.
    *
    * @return The list of OWL IDs.
    */
  def owlKeys: List[OwlID] = OwlStorage.keys

  /**
    * Gets information about an OWL document.
    *
    * @param id The ID of the OWL document.
    * @return Information about the OWL document if found.
    */
  def getOwl(id: OwlID): Option[Owl] = OwlStorage.get(id)

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

          OwlStorage
            .update(id, updatedOwl)
            .flatMap(OwlStorage.get)
            .getOrElse(throw InternalException(s"Failed to update Owl at $id"))
        }
      case None =>
        throw BadRequestException(s"Owl $id not found.")
    }
  }

  /**
    * Deletes an OWL document.
    *
    * @param id The ID of the OWL document.
    * @return Information about the deleted OWL document if successful. Otherwise the exception that
    *         caused the failure.
    */
  def deleteOwl(id: OwlID): Try[Owl] = Try {

    if (OwlStorage.hasDependents(id)) {
      val msg = s"Owl $id cannot be deleted since it has dependents." +
        s"Delete first dependents."
      logger.error(msg)
      throw BadRequestException(msg)
    }

    // TODO: check SsdStorage, OctopusStorage
    OwlStorage.get(id) match {
      case Some(owl) =>
        OwlStorage.remove(id) match {
          case Some(_) =>
            owl
          case None =>
            throw InternalException(s"Owl $id could not be deleted.")
        }
      case None =>
        throw NotFoundException(s"Owl $id not found.")
    }
  }

}
