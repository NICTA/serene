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
package au.csiro.data61.matcher

import org.json4s._
import org.scalatra._
import org.scalatra.json._
import org.scalatra.servlet._
import play.api.libs.json.Json

import scala.language.postfixOps
import scala.util.{Failure, Success, Try}


/**
 * Servlet class to define the integration API
 */
class MatcherServlet extends ScalatraServlet with JacksonJsonSupport with FileUploadSupport {
  protected implicit val jsonFormats: Formats = DefaultFormats

  val APIVersion = "v1.0"

  get(s"/$APIVersion/new") {
    //Message("Hello", "World")
    Json.stringify(Json.toJson(
      Column[String]("junk",  1, 2, List.fill(4)("genAlpha"), LogicalType.STRING)
    ))
  }

  /**
   * Dataset REST endpoints...
   */

  //
  // Returns all dataset keys
  //
  // curl http://localhost:8080/v1.0/dataset
  //
  get(s"/$APIVersion/dataset") {
    MatcherAPI.datasetKeys
  }

  //
  // Adds a new dataset with a description and a user-specified logical typemap.
  // File is required, the others are optional.
  //
  // Returns a JSON DataSet object with id.
  //
  // curl -X POST http://localhost:8080/v1.0/dataset
  //   -F 'file=@foobar/test.csv'
  //   -F 'description=This is the description string'
  //   -F 'typeMap={"col_name":"int", "col_name2":"string", "col_name3":"float"}'
  //
  post(s"/$APIVersion/dataset") {
    Try {
      val req = DataSetParser.processRequest(request)

      if (req.file.isEmpty) throw new BadRequestException("Failed to find 'file' in request.")

      MatcherAPI.createDataset(req)
    } match {
      case Success(ds) =>
        ds
      case Failure(err: BadRequestException) =>
        BadRequest(s"Request failed: ${err.getMessage}")
      case Failure(err) =>
        InternalServerError(s"Failed to upload resource: ${err.getMessage}")
    }
  }

  //
  // Returns a JSON DataSet object at id
  //
  // curl http://localhost:8080/v1.0/dataset/12354687
  //
  get(s"/$APIVersion/dataset/:id") {
    val idStr = params("id")

    val dataset = for {
      id <- Try(idStr.toInt).toOption
      ds <- MatcherAPI.getDataSet(id)
    } yield ds

    dataset getOrElse BadRequest(s"Dataset $idStr does not exist.")
  }

  //
  // Patch a portion of a DataSet. Only description and typeMap
  //
  // Returns a JSON DataSet object at id
  //
  // curl -X PATCH http://localhost:8080/v1.0/dataset/12354687
  //   -F 'description=This is the new description'
  //
  patch(s"/$APIVersion/dataset/:id") {
    val idStr = params("id")

    val req = DataSetParser.processRequest(request)

    if (req.file.nonEmpty) throw new BadRequestException("Forbidden to patch 'file'.")

    val dataset = for {
      id <- Try(idStr.toInt)
      ds <- Try(MatcherAPI.updateDataset(req.description, req.typeMap, id))
    } yield ds

    dataset match {
      case Success(ds) =>
        ds
      case Failure(err) =>
        BadRequest(s"Failed to update dataset $idStr: ${err.getMessage}")
    }
  }

  //
  // Deletes the dataset at position id.
  //
  // curl -X DELETE http://localhost:8080/v1.0/dataset/12354687
  //
  delete(s"/$APIVersion/dataset/:id") {
    val idStr = params("id")

    val dataset = for {
      id <- Try(idStr.toInt)
      ds <- Try(MatcherAPI.deleteDataset(id))
    } yield ds

    dataset match {
      case Success(Some(_)) =>
        Ok
      case Success(None) =>
        NotFound(s"Dataset $idStr could not be found.")
      case Failure(err) =>
        InternalServerError(s"Failed to delete resource $idStr. ${err.getMessage}")
    }
  }

  /**
   * Configuration elements...
   */

  before() {
    contentType = formats("json")
  }

  error {
    case e: SizeConstraintExceededException =>
      RequestEntityTooLarge("File size too large. Please ensure data upload is an octet-stream.")
    case err: Exception =>
      InternalServerError(s"Failed unexpectedly: ${err.getMessage}")
    case _ =>
      InternalServerError(s"Failed spectacularly.")
  }

  //
  // Here we prevent the user from uploading large files. Files
  // need to be uploaded with octet-streams so they can be written
  // directly to files internally.
  //
  configureMultipartHandling(
    MultipartConfig(
      maxFileSize = Some(Long.MaxValue),
      location = Some("/tmp"),
      fileSizeThreshold = Some(1024 * 1024)
    )
  )
}


/**
 * Errors caused by bad requests
 *
 * @param message Error message from the request
 */
class BadRequestException(message: String) extends RuntimeException(message)

/** Error for html request parse errors
 *
 * @param message Error message from the parsing event
 */
class ParseException(message: String) extends RuntimeException(message)
