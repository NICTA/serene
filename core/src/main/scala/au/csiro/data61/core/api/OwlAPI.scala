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

import java.io.File
import java.nio.charset.StandardCharsets

import au.csiro.data61.core.types.ModelerTypes.OwlID
import com.twitter.finagle.http.Response
import com.twitter.finagle.http.exp.Multipart
import com.twitter.io.Buf
import io.finch._
import org.apache.commons.io.FileUtils
import org.joda.time.DateTime
import org.json4s.jackson.JsonMethods._

import scala.language.postfixOps


object OwlDocumentFormat extends Enumeration {
  type OwlDocumentFormat = Value
  val Turtle = Value("turtle")
  val JsonLd = Value("jsonld")
  val RdfXml = Value("rdfxml")
  val Unknown = Value("unknown")
}

case class Owl(
              id: OwlID,
              name: String,
              format: OwlDocumentFormat.OwlDocumentFormat,
              description: String,
              dateCreated: DateTime,
              dateModified: DateTime)


/**
 * Alignment application object. Here we compose the endpoints
 * and serve as a Finagle Http Service forever.
 *
 */
object OwlAPI extends RestAPI {

  // REMOVE!!!!
  val OwlFile = "core/src/test/resources/owl/dataintegration_report_ontology.owl"

  val junkOwl = Owl(
    1,
    "testing.owl",
    OwlDocumentFormat.Turtle,
    "This is a test file",
    DateTime.now(),
    DateTime.now()
  )

  val datasetCreateOptions =
    fileUploadOption("file") ::
      paramOption("description") ::
      paramOption("typeMap")

  val owlRoot: Endpoint[List[Int]] = get(APIVersion :: "owl") {
    Ok(List(junkOwl.id))
  }

  val owlFile: Endpoint[Response] = get(APIVersion :: "owl" :: int :: "file") {
    (id: Int) =>
      val content = FileUtils.readFileToString(new File(OwlFile), StandardCharsets.UTF_8)
      val rep = Response()
      rep.content = Buf.Utf8(content)
      rep.contentType = "text/plain"
      rep
  }

  val owlCreate: Endpoint[Owl] = post(APIVersion :: "owl" :: datasetCreateOptions) {

    (file: Option[Multipart.FileUpload],
     desc: Option[String],
     format: Option[String]) => {

        logger.info(s"Creating ontology file=$file, desc=$desc, format=$format")

        Ok(junkOwl)
      }
  }

  val owlGet: Endpoint[Owl] = get(APIVersion :: "owl" :: int) {
    (id: Int) =>
      Ok(junkOwl)
  }

  val owlUpdate: Endpoint[Owl] = post(APIVersion :: "owl" :: int) {
    (id: Int) =>
      Ok(junkOwl)
  }

  val owlRemove: Endpoint[String] = delete(APIVersion :: "owl" :: int) {
    (id: Int) =>
      Ok("Deleted successfully")
  }

  val endpoints = owlRoot :+: owlCreate :+: owlGet :+: owlUpdate :+: owlRemove :+: owlFile

}
