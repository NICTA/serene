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

import scala.language.postfixOps
import scala.util.{Failure, Success, Try}


/**
 * Servlet class to define the integration API
 */
class MatcherServlet extends ScalatraServlet with JacksonJsonSupport with FileUploadSupport {
  protected implicit val jsonFormats: Formats = DefaultFormats

  val APIVersion = "v1.0"

  get(s"/$APIVersion") {
    Message("Hello", "World")
  }

  /**
   * Dataset REST endpoints...
   */

  get(s"/$APIVersion/dataset") {
    MatcherAPI.datasetKeys
  }

  post(s"/$APIVersion/dataset") {
    Try {
      MatcherAPI.createDataset(request)
    } match {
      case Success(ds) =>
        ds
      case Failure(err: BadRequestException) =>
        BadRequest(s"Request failed: ${err.getMessage}")
      case Failure(err) =>
        InternalServerError(s"Failed to upload resource: ${err.getMessage}")
    }
  }

  get(s"/$APIVersion/dataset/:id") {
    val idStr = params("id")

    val dataset = for {
      id <- Try(idStr.toInt).toOption
      ds <- MatcherAPI.getDataSet(id)
    } yield ds

    dataset getOrElse BadRequest(s"Dataset $idStr does not exist.")
  }

  patch(s"/$APIVersion/dataset/:id") {
    val idStr = params("id")

    val dataset = for {
      id <- Try(idStr.toInt)
      ds <- Try(MatcherAPI.updateDataset(request, id))
    } yield ds

    dataset match {
      case Success(ds) =>
        ds
      case Failure(err) =>
        BadRequest(s"Failed to update dataset $idStr: ${err.getMessage}")
    }
  }

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
   * Config elements...
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


  /**
   * Here we prevent the user from uploading large files. Files
   * need to be uploaded with octet-streams so they can be written
   * directly to files. e.g.
   *
   * curl -i -X POST -H "Content-Type: multipart/form-data"
   *   -F "description=This is a medium length data file, used for testing."
   *   -F 'typeMap={"a":"b", "c":"d", "e":"f"}'
   *   -F "file=@foobar/medium.csv"
   *   http://localhost:8080/v1.0/dataset
   */
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
