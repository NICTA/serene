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

import java.io.{IOException, InputStream}

import au.csiro.data61.core.storage.{AlignmentStorage, OwlStorage}
import au.csiro.data61.core.types.ModelerTypes.OwlDocumentFormat.OwlDocumentFormat
import au.csiro.data61.core.types.ModelerTypes.{AlignmentID, Owl, OwlID}
import org.joda.time.DateTime

import scala.util.{Random, Try}

object ModelerInterface {

  /**
    * Passes the alignment keys up to the API
    *
    * @return
    */
  def alignmentKeys: List[AlignmentID] = {
    AlignmentStorage.keys
  }

  /**
    * Creates an OWL document with related information.
    * @param description The description of the OWL document.
    * @param format The format of the OWL document.
    * @param inputStream The input stream of the OWL document.
    * @return The created OWL information if successful. Otherwise the exception that caused the
    *         failure.
    */
  def createOwl(
      name: String,
      description: String,
      format: OwlDocumentFormat,
      inputStream: InputStream): Try[Owl] = Try {
    val id = Random.nextInt(Integer.MAX_VALUE)
    val now = DateTime.now
    val owl = Owl(
      id = id,
      name = name,
      format = format,
      description = description,
      dateCreated = now,
      dateModified = now
    )

    OwlStorage.add(id, owl) match {
      case Some(_) =>
        OwlStorage.writeOwlDocument(OwlStorage.getOwlDocumentPath(id, format), inputStream).get
        owl
      case None => throw new IOException(s"Owl $id could not be created.")
    }
  }

  /**
    * Gets the IDs of available OWL documents.
    * @return The list of OWL IDs.
    */
  def owlKeys: List[OwlID] = OwlStorage.keys

  /**
    * Gets information about an OWL document.
    * @param id The ID of the OWL document.
    * @return Information about the OWL document if found.
    */
  def getOwl(id: OwlID): Option[Owl] = OwlStorage.get(id)

  /**
    * Updates information about an OWL document.
    * @param id The ID of the OWL document.
    * @param description The description of the OWL document.
    * @return Updated information of the OWL document if successful. Otherwise the exception that
    *         caused the failure.
    */
  def updateOwl(id: OwlID, description: Option[String]): Try[Owl] = Try {
    OwlStorage.get(id) match {
      case Some(owl) =>
        val updatedOwl = owl.copy(
          description = description.getOrElse(owl.description)
        )
        OwlStorage.update(id, updatedOwl) match {
          case Some(_) => updatedOwl
          case None => throw new IOException(s"Owl $id could not be updated.")
        }
      case None => throw new NoSuchElementException(s"Owl $id not found.")
    }
  }

  /**
    * Deletes an OWL document.
    * @param id The ID of the OWL document.
    * @return Information about the deleted OWL document if successful. Otherwise the exception that
    *         caused the failure.
    */
  def deleteOwl(id: OwlID): Try[Owl] = Try {
    OwlStorage.get(id) match {
      case Some(owl) =>
        OwlStorage.remove(id) match {
          case Some(_) => owl
          case None => throw new IOException(s"Owl $id could not be deleted.")
        }
      case None =>
        throw new NoSuchElementException(s"Owl $id not found.")
    }
  }
}
