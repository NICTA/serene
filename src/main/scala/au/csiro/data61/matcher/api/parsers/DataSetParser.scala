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
package au.csiro.data61.matcher.api.parsers

import java.io.InputStream
import javax.servlet.http.{HttpServletRequest, Part}

import au.csiro.data61.matcher.types.DataSetTypes._
import org.json4s.jackson.JsonMethods._
import org.json4s.{DefaultFormats, Formats}

import scala.language.postfixOps
import scala.util.Try

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

/**
 * Parses requests for dataset objects. This can include file
 * streams, typemaps or descriptions.
 */
object DataSetParser {

  protected implicit val jsonFormats: Formats = DefaultFormats

  // constants
  val FilePartName = "file"
  val TypeMapPartName = "typeMap"
  val DescriptionPartName = "description"
  val MissingFile = "unknown"

  /**
   * Parses the request to pull the typeMap out from the multipart form
   *
   * @param request The HttpServletRequest object with the current request
   * @return
   */
  protected def extractTypeMap(request: HttpServletRequest): Option[TypeMap] = {
    (for {
      desc <- Try {
        multiPartAsString(request.getPart(TypeMapPartName))
      }
      typeMap <- Try {
        parse(desc).extract[Map[String, String]]
      }
    } yield typeMap) toOption
  }

  /**
   * Extracts the description from the original servlet multipart request.
   *
   * @return
   */
  protected def extractDescription(request: HttpServletRequest): Option[String] = {
    Try {
      multiPartAsString(request.getPart(DescriptionPartName))
    } toOption
  }

  /**
   * Extracts the filestream and name from the original servlet multipart request.
   *
   * @param request Original request from the server
   * @return
   */
  protected def extractFile(request: HttpServletRequest): Option[FileStream] = {
    // grab filename from upload headers...

    request.getPart(FilePartName) match {
      case fp: Part =>
        val fs = FileStream(extractFileName(fp), fp.getInputStream)
        Some(fs)
      case _ =>
        None
    }
  }

  /**
   * Extracts the filename out from the content-disposition
   * header.
   *
   * @param filePart The html multi-part file Part
   * @return
   */
  private def extractFileName(filePart: Part): String = {
    val str = filePart.getHeader("content-disposition")
    val pattern = """filename=\"(.*)\"""".r
    pattern.findFirstMatchIn(str) match {
      case Some(m) =>
        m.group(1)
      case _ =>
        // no filename was present....
        MissingFile
    }
  }

  /**
   * Pulls a string out from a request into memory. Here it is used
   * for short data requests inside the multipart file upload.
   *
   * @param part Form data upload part
   * @return
   */
  protected def multiPartAsString(part: Part): String = {
    scala.io.Source.fromInputStream(part.getInputStream).getLines.mkString("")
  }

  /**
   * Extracts a file, description and typeMap from the servlet multi-param
   * request. Here we don't use the default FileUploadHelper because it will
   * attempt to read everything into memory. This is not usable for
   * large files. Instead we pipe the inputStream directly to a file.
   * In addition the MultiParam object in Scalatra allows the disk to
   * be used as a buffer.
   *
   * @param request The original HttpServlet request object
   * @return A request object to be parsed by the integration API
   */
  def processRequest(request: HttpServletRequest): DataSetRequest = {

    DataSetRequest(
      file = extractFile(request),
      description = extractDescription(request),
      typeMap = extractTypeMap(request))
  }
}
