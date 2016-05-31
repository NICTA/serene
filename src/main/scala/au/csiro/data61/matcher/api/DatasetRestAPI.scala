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
package au.csiro.data61.matcher.api

import java.io.FileInputStream

import au.csiro.data61.matcher.DataSetTypes._
import au.csiro.data61.matcher._
import com.twitter.finagle.http.exp.Multipart
import com.twitter.finagle.http.exp.Multipart.{InMemoryFileUpload, OnDiskFileUpload}
import io.finch._
import org.json4s.jackson.JsonMethods._

import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

/**
 * Dataset REST endpoints...
 *
 *  GET    /v1.0/dataset
 *  POST   /v1.0/dataset      -- file (binary), description (string), typeMap (obj(string->string))
 *  GET    /v1.0/dataset/:id
 *  POST   /v1.0/dataset/:id  -- description (string), typeMap (obj(string->string))
 *  DELETE /v1.0/dataset/:id
 */
object DatasetRestAPI extends RestAPI {

  /**
   * Returns all dataset keys
   *
   * curl http://localhost:8080/v1.0/dataset
   */
  val datasetRoot: Endpoint[List[Int]] = get(APIVersion :: "dataset")({
    Ok(MatcherInterface.datasetKeys)
  })

  /**
   * Adds a new dataset with a description and a user-specified logical typemap.
   * File is required, the others are optional.
   *
   * Returns a JSON DataSet object with id.
   *
   * curl -X POST http://localhost:8080/v1.0/dataset
   *   -F 'file=@foobar/test.csv'
   *   -F 'description=This is the description string'
   *   -F 'typeMap={"col_name":"int", "col_name2":"string", "col_name3":"float"}'
   */
  val datasetCreateOptions =
    fileUploadOption("file") ::
      paramOption("description") ::
      paramOption("typeMap")

  val datasetCreate: Endpoint[DataSet] = post(APIVersion :: "dataset" :: datasetCreateOptions) {

    (file: Option[Multipart.FileUpload],
     desc: Option[String],
     typeMap: Option[String]) =>
      file match {
        case Some(OnDiskFileUpload(buffer, contentType, fileName, _)) =>
          val req = DataSetRequest(
            Some(FileStream(fileName, new FileInputStream(buffer))),
            desc,
            for {
              str <- typeMap
              tm <- Try { parse(str).extract[TypeMap] } toOption
            } yield tm
          )
          val ds = MatcherInterface.createDataset(req)
          Ok(ds)
        case Some(InMemoryFileUpload(_, _, _, _))=>
          InternalServerError(InternalException("Can't deal with in memory!!"))
        case _ =>
          BadRequest(BadRequestException("File missing from multipart form request."))
      }
  }

  /**
   * Returns a JSON DataSet object at id
   *
   * curl http://localhost:8080/v1.0/dataset/12354687
   */
  val datasetGet: Endpoint[DataSet] = get(APIVersion :: "dataset" :: int) {
    (id: Int) =>
      MatcherInterface.getDataSet(id) match {
        case Some(ds) =>
          Ok(ds)
        case _ =>
          NotFound(NotFoundException(s"Dataset $id does not exist."))
      }
  }

  /**
   * Patch a portion of a DataSet. Only description and typeMap
   *
   * WARNING: The multipart form patch cannot be handled by Finagle???
   * Instead a data binary request must be sent with x-application-form???
   * And must be set as a post due to lack of request builders????
   *
   * Returns a JSON DataSet object at id
   *
   * curl -X POST -d 'description=This is the new description'
   * http://localhost:8080/v1.0/dataset/12354687
   */
  val datasetPatchOptions =
    paramOption("description") ::
      paramOption("typeMap") ::
      header("Content-Type")

  val datasetPatch: Endpoint[DataSet] = post(APIVersion :: "dataset" :: int :: datasetPatchOptions) {

    (id: Int, desc: Option[String], typeMap: Option[String], contentType: String) =>

      if (!contentType.contains("x-www-form-urlencoded")) {

        BadRequest(BadRequestException("Request error. PATCH request requires x-www-form-urlencoded header."))

      } else {

        // here we can potentially fail parsing the typemap OR
        // in the update dataset...
        (for {
          tm <- Try {
            typeMap.map(parse(_).extract[TypeMap])
          }
          ds <- Try {
            MatcherInterface.updateDataset(id, desc, tm)
          }
        } yield ds)
        match {
          case Success(ds) =>
            Ok(ds)
          case Failure(err) =>
            InternalServerError(InternalException(err.getMessage))
        }
      }
  }

  /**
   * Deletes the dataset at position id.
   *
   * curl -X DELETE http://localhost:8080/v1.0/dataset/12354687
   */
  val datasetDelete: Endpoint[String] = delete(APIVersion :: "dataset" :: int) {
    (id: Int) =>
      Try(MatcherInterface.deleteDataset(id)) match {
        case Success(Some(_)) =>
          logger.debug(s"Deleted dataset $id")
          Ok(s"Dataset $id deleted successfully.")
        case Success(None) =>
          logger.debug(s"Could not find dataset $id")
          NotFound(NotFoundException(s"Dataset $id could not be found"))
        case Failure(err) =>
          logger.debug(s"Some other problem with deleting...")
          InternalServerError(InternalException(s"Failed to delete resource: ${err.getMessage}"))
      }
  }

  /**
   * Final endpoints for the Dataset endpoint...
   */
  val endpoints =
    datasetRoot :+:
    datasetCreate :+:
    datasetGet :+:
    datasetPatch :+:
    datasetDelete
}
