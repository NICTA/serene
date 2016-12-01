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

import au.csiro.data61.core.api._
import au.csiro.data61.core.types.{MatcherJsonFormats}
import com.typesafe.scalalogging.LazyLogging

import scala.language.postfixOps
import com.twitter.util.Await
import com.twitter.finagle.{ListeningServer, Http}

import io.finch._
import io.finch.json4s._


object Serene extends LazyLogging with MatcherJsonFormats with RestAPI {

  val Address = Config.ServerAddress

  val endpoints =
    DatasetAPI.endpoints :+:
      ModelAPI.endpoints :+:
      AlignmentAPI.endpoints :+:
      OwlAPI.endpoints :+:
      SsdAPI.endpoints :+:
      TestAPI.endpoints

  val restAPI = endpoints.handle {
    case e @ InternalException(msg) =>
      logger.error(s"Internal server error: $msg")
      InternalServerError(e)
    case e @ ParseException(msg) =>
      logger.error(s"Parse exception error: $msg")
      InternalServerError(e)
    case e @ BadRequestException(msg) =>
      logger.error(s"Bad request exception: $msg")
      BadRequest(e)
    case e @ NotFoundException(msg) =>
      logger.error(s"Resource not found exception: $msg")
      NotFound(e)
    case e: Exception =>
      logger.error(s"Error: ${e.getMessage}")
      InternalServerError(e)
  }

  def defaultServer: ListeningServer = Http.serve(Address, restAPI.toService)

  def main(args: Array[String]): Unit = {
    logger.info("Start HTTP server on port " + Address)
    Await.ready(defaultServer)
  }
}
