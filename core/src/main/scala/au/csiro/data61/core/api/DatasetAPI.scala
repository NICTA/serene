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

import java.io.{ByteArrayInputStream, FileInputStream, InputStream}

import au.csiro.data61.core.drivers.DataSetInterface
import au.csiro.data61.types.{DataSet, DataSetTypes}
import DataSetTypes._
import com.twitter.finagle.http.exp.Multipart
import com.twitter.finagle.http.exp.Multipart.{InMemoryFileUpload, OnDiskFileUpload}
import com.twitter.io.{Buf, BufReader}
import com.twitter.util.Await
import io.finch._
import org.apache.commons.io.FilenameUtils
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
object DatasetAPI extends RestAPI {

  /**
   * Returns all dataset keys
   *
   * curl http://localhost:8080/v1.0/dataset
   */
  val datasetRoot: Endpoint[List[Int]] = get(APIVersion :: "dataset") {
    Ok(DataSetInterface.storageKeys)
  }

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

      logger.info(s"Creating dataset file=$file, desc=$desc, typeMap=$typeMap")

      /**
        * Helper function to create the dataset from a filestream...
        *
        * @param fs
        * @param filename
        * @return
        */
      def createDataSet(fs: FileStream, filename: String) = {
          val req = DataSetRequest(
            Some(fs),
            desc,
            for {
              str <- typeMap
              tm <- Try {
                parse(str).extract[TypeMap]
              } toOption
            } yield tm
          )
          DataSetInterface.createDataset(req)
      }

      // main matching object...
      file match {

        case Some(OnDiskFileUpload(buffer, _, fileName, _)) =>
          logger.info("   on disk file upload")
          val fs = FileStream(fileName, new FileInputStream(buffer))
          Ok(createDataSet(fs, fileName))

        case Some(InMemoryFileUpload(buffer, _, fileName, _)) =>
          logger.info("   in memory")
          // first we need to convert from the twitter Buf object...
          val bytes = Buf.ByteArray.Owned.extract(buffer)
          // next we convert to a filestream as before...
          val fs = FileStream(fileName, new ByteArrayInputStream(bytes))
          Ok(createDataSet(fs, fileName))

        case _ =>
          BadRequest(BadRequestException("File missing from multipart form request."))
      }
  }

  /**
   * Returns a JSON DataSet object at id
   *
   * curl http://localhost:8080/v1.0/dataset/12354687
   */
  val datasetGet: Endpoint[DataSet] = get(APIVersion :: "dataset" :: int :: paramOption("samples")) {
    (id: Int, samples: Option[String]) =>

      logger.debug(s"Get dataset id=$id, samples=$samples")

      val dataset = for {
        sc <- Try(samples.map(_.toInt))
        ds <- Try(DataSetInterface.getDataSet(id, sc))
      } yield ds

      dataset match {
        case Success(Some(ds))  =>
          Ok(ds)
        case Success(None) =>
          NotFound(NotFoundException(s"Dataset $id does not exist."))
        case Failure(err) =>
          BadRequest(BadRequestException(err.getMessage))
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

      logger.debug(s"Patching dataset id=$id, desc=$desc, typeMap=$typeMap")

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
            DataSetInterface.updateDataset(id, desc, tm)
          }
        } yield ds)
        match {
          case Success(ds) =>
            Ok(ds)
          case Failure(err: NotFoundException) =>
            NotFound(err)
          case Failure(err: BadRequestException) =>
            BadRequest(err)
          case Failure(err: InternalException) =>
            InternalServerError(err)
          case Failure(err) =>
            logger.error(s"Some other problem with updating dataset $id: ${err.getMessage}")
            InternalServerError(InternalException(s"Failed to update dataset $id."))
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
      Try(DataSetInterface.delete(id)) match {
        case Success(Some(_)) =>
          logger.debug(s"Deleted dataset $id")
          Ok(s"Dataset $id deleted successfully.")
        case Success(None) =>
          logger.debug(s"Could not find dataset $id")
          NotFound(NotFoundException(s"Dataset $id could not be found"))
        case Failure(err: BadRequestException) =>
          BadRequest(err)
        case Failure(err: InternalException) =>
          InternalServerError(err)
        case Failure(err) =>
          logger.error(s"Some other problem with deleting dataset $id: ${err.getMessage}")
          InternalServerError(InternalException(s"Failed to delete dataset."))
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

/**
 * Holds the filestream from the incoming request.
 *
 * @param name Name of the file as parsed from the request
 * @param stream Java InputFileStream pointing to the octet stream
 */
case class FileStream(name: String, stream: InputStream)

/**
 * Object returned from any dataset request. Optionally include
 * a filestream, description or a typeMap. Whether missing values
 * throw an error is up to the request handler.
 *
 * @param file Filestream object pointing to the octet stream
 * @param description Description field written for the dataset
 * @param typeMap Map of column logical types contained in the file
 */
case class DataSetRequest(file: Option[FileStream],
                          description: Option[String],
                          typeMap: Option[TypeMap])
