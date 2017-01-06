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

import au.csiro.data61.matcher.api.{DatasetRestAPI, RestAPI}
import au.csiro.data61.matcher.types.{MatcherJsonFormats, Message, StatusMessage, VersionMessage}
import com.typesafe.scalalogging.LazyLogging

import scala.language.postfixOps
import com.twitter.util.Await
import com.twitter.finagle.{Http, ListeningServer}
import io.finch._
import io.finch.json4s._
import api._

/**
 * Main application object. Here we compose the endpoints
 * and serve as a Finagle Http Service forever.
 *
 */
object Matcher extends LazyLogging with MatcherJsonFormats {

  val Address = Config.ServerAddress

  val components =
    DatasetRestAPI.endpoints :+:
      ModelRestAPI.endpoints :+:
      TestRestAPI.endpoints

  val restAPI = components.handle {
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


object TestRestAPI extends RestAPI {

//  val status: Endpoint[StatusMessage] = get(APIVersion) {
//    Ok(StatusMessage("ok"))
//  }

  val version: Endpoint[VersionMessage] = get(/) {
    Ok(VersionMessage(APIVersion))
  }

  val asdf: Endpoint[Message] = get(APIVersion :: "asdf") {
    Ok(Message("hello", "asdf"))
  }

  val qwer: Endpoint[Message] = get(APIVersion) {
    Ok(Message("hello", "world"))
  }

  val endpoints = asdf :+: qwer :+: version
}
