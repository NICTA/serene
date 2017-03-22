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
package au.csiro.data61.core.api

import java.nio.file.Files
import au.csiro.data61.core.drivers.OwlInterface
import au.csiro.data61.types.SsdTypes.{Owl, OwlID, OwlDocumentFormat}
import com.twitter.finagle.http.Version.Http11
import com.twitter.finagle.http.{Response, Status, Version}
import com.twitter.finagle.http.exp.Multipart.{FileUpload, InMemoryFileUpload, OnDiskFileUpload}
import com.twitter.io.{Reader, BufInputStream}
import io.finch._


import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

/**
 * Alignment application object. Here we compose the endpoints
 * and serve as a Finagle Http Service forever.
 *
 */
object OwlAPI extends RestAPI {
  protected val OwlRootPath = "owl"
  protected val UrlEncodedFormContentType = "application/x-www-form-urlencoded"

  /**
    * Lists keys of all OWLs.
    *
    * This endpoint handles GET requests for /version/owl.
    */
  val listOwls: Endpoint[List[OwlID]] = get(APIVersion :: OwlRootPath) {
    Ok(OwlInterface.storageKeys)
  }

  /**
    * Creates an OWL.
    *
    * This endpoint handles POST requests for
    * /version/owl?format=:format&description=[:description]. The request body should be
    * multipart/form-data containing the OWL document with name "file".
    */
  val createOwl: Endpoint[Owl] = post(APIVersion :: OwlRootPath :: fileUpload("file") :: param("format") :: paramOption("description")) {

    (file: FileUpload, format: String, description: Option[String]) =>

      logger.info(s"Creating OWL with file=$file, format=$format, description=$description.")

      val name = file.fileName
      val desc = description.getOrElse("")
      Try { OwlDocumentFormat.withName(format) } match {
        case Failure(ex) =>
          logger.info(s"The format $format of OWL $file is not supported.", ex)
          BadRequest(BadRequestException("The format of OWL is not supported."))
        case Success(fmt) =>
          val stream = file match {
            case OnDiskFileUpload(content, _, _, _) =>
              Files.newInputStream(content.toPath)
            case InMemoryFileUpload(content, _, _, _) =>
              new BufInputStream(content)
          }

          OwlInterface.createOwl(name, desc, fmt, stream) match {
            case Some(owl: Owl) =>
              Ok(owl)
            case _ =>
              logger.error(s"Owl could not be created.")
              InternalServerError(InternalException(s"Owl could not be created."))
          }
      }
  }

  /**
    * Gets the OWL with specified ID.
    *
    * The endpoint handles GET requests for /version/owl/:id.
    */
  val getOwl: Endpoint[Owl] = get(APIVersion :: OwlRootPath :: int) { (id: Int) =>
    logger.info(s"Getting OWL with ID=$id")

    OwlInterface.get(id) match {
      case Some(owl) => Ok(owl)
      case None => NotFound(NotFoundException(s"OWL $id not found"))
    }
  }


  val getOwlDocument: Endpoint[Response] = get(APIVersion :: OwlRootPath :: int :: "file") {
    (id: Int) =>
      logger.info(s"Getting OWL document with ID=$id")

      OwlInterface.get(id) match {
        case Some(owl) =>
          OwlInterface.getOwlDocument(owl) match {
            case Success(reader: Reader) =>
              val response = Response(Http11, Status.Ok, reader)
              response.contentType = "text/plain"
              response
            case Failure(th) =>
              logger.error(s"Failed to get OWL document ${owl.id}.", th)
              Response(Status.InternalServerError)
          }
        case None =>
          Response(Status.NotFound)
      }
  }

  /**
    * Updates the OWL with specified ID.
    *
    * This endpoint handles POST requests for /version/owl/:id with an
    * application/x-www-form-urlencoded body containing an optional parameter "description".
    */
  val updateOwl: Endpoint[Owl] = post(APIVersion :: OwlRootPath :: int :: fileUploadOption("file") :: paramOption("description") :: header("Content-Type")) {

    (id: Int, file: Option[FileUpload], description: Option[String], contentType: String) =>

      logger.info(s"Updating OWL with ID=$id, file=$file, description=$description")

      val stream = file.map {
        case OnDiskFileUpload(content, _, _, _) =>
          Files.newInputStream(content.toPath)
        case InMemoryFileUpload(content, _, _, _) =>
          new BufInputStream(content)
      }

      val filename = file.map(_.fileName)

      OwlInterface.updateOwl(id, description, filename, stream) match {
        case Success(owl) =>
          Ok(owl)
        case Failure(th) =>
          InternalServerError(new RuntimeException(th))
      }
  }

  /**
    * Deletes the OWL with specified ID.
    *
    * This endpoint handles DELETE requests for /version/owl/:id.
    */
  val deleteOwl: Endpoint[String] = delete(APIVersion :: OwlRootPath :: int) {
    (id: Int) =>
      logger.info(s"Deleting OWL with ID=$id")

      Try(OwlInterface.delete(id)) match {
        case Success(Some(_)) =>
          logger.debug(s"Deleted ontology $id")
          Ok(s"Ontology $id deleted successfully.")
        case Success(None) =>
          logger.debug(s"Could not find ontology $id")
          NotFound(NotFoundException(s"Ontology $id could not be found"))
        case Failure(err: BadRequestException) =>
          BadRequest(err)
        case Failure(err: InternalException) =>
          InternalServerError(err)
        case Failure(err) =>
          logger.error(s"Some other problem with deleting: ${err.getMessage}")
          InternalServerError(InternalException(s"Failed to delete ontology $id"))
      }
  }

  /**
    * Represents all OWL endpoints.
    */
  val endpoints = listOwls :+:
    createOwl :+:
    getOwl :+:
    updateOwl :+:
    deleteOwl :+:
    getOwlDocument
}
