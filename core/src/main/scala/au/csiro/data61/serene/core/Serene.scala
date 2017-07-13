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
package au.csiro.data61.serene.core

import java.util.Calendar

import au.csiro.data61.serene.core.api._
import au.csiro.data61.serene.core.storage.JsonFormats
import com.twitter.conversions.storage._
import com.twitter.finagle.{Http, ListeningServer}
import com.twitter.util.Await
import com.typesafe.scalalogging.LazyLogging
import io.finch._
import io.finch.json4s._
import scala.language.postfixOps

/**
  * Main object for Serene server. We use the App object to access the
  * cmd line args ahead of the main function. We then combine all
  * Finch endpoints and protect with high level error handlers, then
  * serve the API.
  */
object Serene extends LazyLogging with JsonFormats with RestAPI {

  val version = Option(getClass.getPackage.getImplementationVersion)

  println("<<<<<<<<<<<<<")
  println(getClass.getPackage)
  println(getClass.getPackage.getSpecificationVersion)
  println(getClass.getPackage.getImplementationTitle)
  println(getClass.getPackage.getImplementationVersion)

  def echo(msg: String) { Console println msg }

  // Initialize with defaults....
  logger.info("Initializing with default params...")
  var config = Config(args = Array.empty[String])

  // the full api handler
  val endpoints =
      TestAPI.endpoints

  val restAPI = {
    endpoints.handle {
      case e@InternalException(msg) =>
        logger.error(s"Internal server error: $msg")
        InternalServerError(e)
      case e@ParseException(msg) =>
        logger.error(s"Parse exception error: $msg")
        InternalServerError(e)
      case e@BadRequestException(msg) =>
        logger.error(s"Bad request exception: $msg")
        BadRequest(e)
      case e@NotFoundException(msg) =>
        logger.error(s"Resource not found exception: $msg")
        NotFound(e)
      case e: Exception =>
        logger.error(s"Error: ${e.getMessage}")
        InternalServerError(e)
    }
  }

  // the server address
  def serverAddress: String = s"${config.serverHost}:${config.serverPort}"

  /**
    * The default server object.
    * @return
    */
  def defaultServer: ListeningServer = {
    Http.server
      .withMaxRequestSize(1999.megabytes)
      .serve(s"${config.serverHost}:${config.serverPort}", restAPI.toServiceAs[Application.Json])
  }

  /**
    * Main function to launch the server...
    * @param args Argument string, see the `Config` object for more details`
    */
  def main(args: Array[String]): Unit = {

    // start the server...
    logger.info("Reading command line args...")

    config = Config(args)

    logger.info(s"Start HTTP server on $serverAddress")

    echo(raw"""*
      |*  ${Calendar.getInstance.getTime}
      |*
      |*
      |*    __|   _ \   __|  _ \   __ \   _ \
      |*  \__     __/  |     __/  |   |   __/
      |*  ____/ \___| _|   \___| _|  _| \___|
      |*
      |*
      |*  Version: ${version.getOrElse("internal")}
      |*  Host: ${config.serverHost}
      |*  Port: ${config.serverPort}
      |*""".stripMargin)

    Await.ready(defaultServer)
  }
}
