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
package au.csiro.data61.core

import java.io.File

import au.csiro.data61.core.api.SsdRequest
import au.csiro.data61.core.storage.JsonFormats
import au.csiro.data61.types.{DataSet, Ssd, SsdMapping}
import au.csiro.data61.types.DataSetTypes.DataSetID
import au.csiro.data61.types.ModelTypes.ModelID
import au.csiro.data61.types.SsdTypes.{Owl, OwlID, SsdID}
import au.csiro.data61.types.SsdTypes.OwlDocumentFormat.OwlDocumentFormat
import com.twitter.finagle.{Http, http}
import com.twitter.finagle.http._
import com.twitter.finagle.http.Status._
import com.twitter.finagle.http.Method.{Post}
import com.twitter.io.Buf.ByteArray
import com.twitter.util.{Await, Closable}
import com.twitter.io.{Buf, Reader}
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization.write
import com.typesafe.scalalogging.LazyLogging

import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

/**
 * This launches a test server on localhost...
 */
class TestServer extends LazyLogging with JsonFormats {

  // This is used to pass itself to tests implicitly
  implicit val s = this

  val server = Serene.defaultServer
  val client = Http.newService(Serene.serverAddress)

  val JsonHeader = "application/json"

  def fullUrl(path: String): String = s"http://${Serene.serverAddress}$path"

  /**
   * Helper function to build a get request
   *
   * @param path URL path of the resource
   * @return Http response object
   */
  def get(path: String): Response = {
    val request = http.Request(http.Method.Get, path)
    Await.result(client(request))
  }

  /**
   * Helper function to build a delete request
   *
   * @param path URL of the endpoint to delete
   * @return
   */
  def delete(path: String): Response = {
    val request = http.Request(http.Method.Delete, path)
    Await.result(client(request))
  }

  /**
   * Close the server and client after each test
   */
  def assertClose(): Unit = {
    Closable.all(server, client).close()
  }

  //============ tons of methods to creat/delete resources used throughout all tests
  def requestOwlCreation(document: File,
                         format: OwlDocumentFormat,
                         description: String = "test")
                        (implicit version: String): Try[(Status, String)] = Try {
    val buf = Await.result(Reader.readAll(Reader.fromFile(document)))
    val request = RequestBuilder()
      .url(s.fullUrl(s"/$version/owl"))
      .addFormElement("format" -> format.toString)
      .addFormElement("description" -> description)
      .add(FileElement("file", buf, None, Some(document.getName)))
      .buildFormPost(multipart = true)
    val response = Await.result(s.client(request))
    (response.status, response.contentString)
  }

  def createOwl(document: File,
                format: OwlDocumentFormat,
                description: String = "example")
               (implicit version: String): Try[Owl] =
    requestOwlCreation(document, format, description).map {
      case (Ok, content) => parse(content).extract[Owl]
    }

  /**
    * Posts a request to build a dataset, then returns the DataSet object it created
    * wrapped in a Try.
    * @param document file of the csv resource
    * @param description description string
    * @param typeMap string for the type map which is a map
    * @param version implicit string for the API version
    * @return
    */
  def createDataset(document: File,
                    description: String="test",
                    typeMap: String = "{}")
                   (implicit version: String): Try[DataSet] = Try {
    val buf = Await.result(Reader.readAll(Reader.fromFile(document)))
    val request = RequestBuilder()
      .url(s.fullUrl(s"/$version/dataset"))
      .addFormElement("description" -> description)
      .add(FileElement("file", buf, None, Some(document.getName)))
      .buildFormPost(multipart = true)
    val response = Await.result(s.client(request))
    parse(response.contentString).extract[DataSet]
  }

  def deleteDataset(id: DataSetID)(implicit version: String): Response = {
    logger.info(s"Deleting dataset $id")
    delete(s"/$version/dataset/$id")
  }

  def deleteAllDatasets(implicit version: String): Unit = {
    val request = Request(s"/$version/dataset")
    val response = Await.result(s.client(request))
    parse(response.contentString).extract[List[DataSetID]].foreach(deleteDataset)
  }

  def getAllDatasets(implicit version: String): List[DataSetID] = {
    val request = Request(s"/$version/dataset")
    val response = Await.result(s.client(request))
    parse(response.contentString).extract[List[DataSetID]]
  }

  def requestSsdCreation(document: SsdRequest)
                        (implicit version: String): Try[(Status, String)] = Try {
    val request = Request(Post, s"/$version/ssd")
    request.content = ByteArray(write(document).getBytes: _*)
    request.contentType = "application/json"
    val response = Await.result(s.client(request))
    (response.status, response.contentString)
  }

  def createSsd(document: SsdRequest)(implicit version: String): Try[Ssd] = Try {
    val (_, content) = requestSsdCreation(document).get
    parse(content).extract[Ssd]
  }

  def createSsd(datasetDocument: File, ssdDocument: File)
               (implicit version: String): Try[(SsdRequest, Ssd)] = Try {
    val dataset = createDataset(datasetDocument, description = "ref dataset").get
    val request = parse(ssdDocument).extract[SsdRequest].copy(mappings = Some(SsdMapping(Map(
      dataset.columns.head.id -> 1,
      dataset.columns(1).id -> 3,
      dataset.columns(2).id -> 5,
      dataset.columns(3).id -> 7
    ))))
    (request, createSsd(request).get)
  }

  def requestOwlDeletion(id: OwlID)(implicit version: String): Try[(Status, String)] = Try {
    val response = delete(s"/$version/owl/$id")
    (response.status, response.contentString)
  }

  def listOwls(implicit version: String): Try[List[OwlID]] = Try {
    val request = Request(s"/$version/owl")
    val response = Await.result(s.client(request))
    parse(response.contentString).extract[List[OwlID]]
  }

  def deleteAllOwls(implicit version: String): Unit =
    listOwls.get.map(requestOwlDeletion).foreach(_.get)

  def getOwl(id: OwlID)(implicit version: String): Try[Owl] = Try {
    val request = Request(s"/$version/owl/$id")
    val response = Await.result(s.client(request))
    parse(response.contentString).extract[Owl]
  }

  def listSsds(implicit version: String): Try[List[SsdID]] = Try {
    val request = Request(s"/$version/ssd")
    val response = Await.result(s.client(request))
    parse(response.contentString).extract[List[SsdID]]
  }

  /**
    *
    * @param id
    * @param version
    * @return
    */
  def requestSsdDeletion(id: SsdID)(implicit version: String): Try[(Status, String)] = Try {
    val response = delete(s"/$version/ssd/$id")
    (response.status, response.contentString)
  }

  def deleteAllSsds(implicit version: String): Unit =
    listSsds.get.map(requestSsdDeletion).foreach(_.get)

  def deleteModel(id: ModelID)(implicit version: String): Response = {
    logger.info(s"Deleting model $id")
    delete(s"/$version/model/$id")
  }

  def getSsd(id: SsdID)(implicit version: String): Try[Ssd] = Try {
    val request = Request(s"/$version/ssd/$id")
    val response = Await.result(s.client(request))
    parse(response.contentString).extract[Ssd]
  }

  /**
    * Deletes all the models from the server. Assumes that
    * the IDs are stored as positive integers
    *
    * @param version Implicit version
    */
  def deleteAllModels(implicit version: String): Unit = {
    val request = Request(s"/$version/model")
    val response = Await.result(s.client(request))
    parse(response.contentString).extract[List[ModelID]].foreach(deleteModel)
  }

}
