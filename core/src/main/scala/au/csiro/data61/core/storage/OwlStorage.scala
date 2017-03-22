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

import java.io.{FileInputStream, InputStream}
import java.nio.file.StandardCopyOption.REPLACE_EXISTING
import java.nio.file.{Files, Path, Paths}

import au.csiro.data61.core.Serene
import au.csiro.data61.types.SsdTypes.OwlDocumentFormat.OwlDocumentFormat
import au.csiro.data61.types.SsdTypes._
import org.json4s.jackson.JsonMethods.parse

import scala.util.{Failure, Success, Try}

/**
  * Stores OWL document and related information.
  */
object OwlStorage extends Storage[OwlID, Owl] {
  val DocumentFileName: String = "document"

  override implicit val keyReader: Readable[Int] = Readable.ReadableInt

  override protected def rootDir: String =
    Paths.get(Serene.config.storageDirs.owl).toAbsolutePath.toString

  override protected def extract(stream: FileInputStream): Owl = parse(stream).extract[Owl]

  /**
    * Gets the path to the OWL document file.
    *
    * @param id The ID of the OWL document.
    * @return The path to the OWL document file.
    */
  def getOwlDocumentPath(id: Int): Option[Path] = {
    get(id).map ( x =>
      getPath(id).resolveSibling(s"$id-$DocumentFileName.${x.format}")
    )
  }

  /**
    * Writes the OWL document with the input stream.
    *
    * Existing document file will be overwritten.
    *
    * @param owlDocumentPath The path to the OWL document file.
    * @param inputStream The input stream.
    */
  def writeOldOwlDocument(owlDocumentPath: Path, inputStream: InputStream): Try[Path] = Try {
    owlDocumentPath.getParent.toFile.mkdirs
    Files.copy(inputStream, owlDocumentPath, REPLACE_EXISTING)
    owlDocumentPath
  }

  /**
    * Writes the OWL document with the input stream.
    *
    * Existing document file will be overwritten.
    *
    * @param id id of the ontology
    * @param inputStream The input stream.
    */
  def writeOwlDocument(id: OwlID, inputStream: InputStream): Option[Path] = {
    val owlDocumentPath = getOwlDocumentPath(id)
    Try {
      owlDocumentPath.foreach(_.getParent.toFile.mkdirs)
      owlDocumentPath.foreach(Files.copy(inputStream, _, REPLACE_EXISTING))
      owlDocumentPath
    } match {
      case Success(_) =>
        owlDocumentPath
      case Failure(err) =>
        logger.error(s"Failed to write owl file for $id.")
        None
    }
  }
}
