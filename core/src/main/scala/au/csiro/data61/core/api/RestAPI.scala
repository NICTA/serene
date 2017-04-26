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

import au.csiro.data61.core.storage.JsonFormats
import au.csiro.data61.types.Exceptions.TypeException
import com.typesafe.scalalogging.LazyLogging
import io.finch._
import com.twitter.io.Buf
import org.json4s.MappingException

/**
 * Generic RestAPI endpoint. Here it just holds the implicit
 * format object, the version number, and the endpoints to
 * be defined by child objects.
 */
trait RestAPI extends LazyLogging with JsonFormats {
  implicit val formats = json4sFormats

  implicit val e: Encode.Aux[MappingException, Text.Html] = Encode.instance((e, cs) =>
    Buf.Utf8(s"<h1>Bad thing happened!!!!!!!!<h1>")
  )

  val APIVersion = "v1.0"

  val endpoints: Endpoint[_]
}

/**
 * Errors caused by bad requests
 *
 * @param message Error message from the request
 */
case class BadRequestException(message: String) extends RuntimeException(message)

/**
 * Error for missing resource
 *
 * @param message Error message from the parsing event
 */
case class NotFoundException(message: String) extends RuntimeException(message)


/**
 * Error for html request parse errors
 *
 * @param message Error message from the parsing event
 */
case class ParseException(message: String) extends RuntimeException(message)

/**
 * Error for internal problems
 *
 * @param message Error message from the lower levels
 */
case class InternalException(message: String) extends RuntimeException(message)