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
package au.csiro.data61.types

import au.csiro.data61.types.Exceptions.TypeException
import com.typesafe.scalalogging.LazyLogging
import org.joda.time.DateTime
import org.json4s._

import scala.util.{Failure, Success, Try}

/**
 * An object that has a property id which is of type key.
 * This is used as the base type for the id-able case classes
 * such as DataSet and Model
 *
 * @tparam Key The id type, nominally an Int or Long
 */
trait Identifiable[Key] {
  def id: Key
}


object HelperJSON extends LazyLogging {
  implicit val formats = DefaultFormats + HelperLinkSerializer + SsdNodeSerializer + FeaturesConfigSerializer
  /**
    * Helper function to parse json objects. This will return None if
    * nothing is present, and throw a BadRequest error if it is incorrect,
    * and Some(T) if correct
    *
    * @param label The key for the object. Must be present in jValue
    * @param jValue The Json Object
    * @tparam T The return type
    * @return
    */
  def parseOption[T: Manifest](label: String, jValue: JValue): Try[Option[T]] = {
    val jv = jValue \ label
    if (jv == JNothing) {
      Success(None)
    } else {
      Try {
        Some(jv.extract[T])
      } recoverWith {
        case err =>
          logger.error(s"Failed to parse: $label. Error: ${err.getMessage}")
          Failure(
            TypeException(s"Failed to parse: $label. Error: ${err.getMessage}"))
      }
    }
  }
}

case object JodaTimeSerializer extends CustomSerializer[DateTime](format => (
  {
    case jv: JValue =>
      implicit val formats = DefaultFormats
      val str = jv.extract[String]
      DateTime.parse(str)
  }, {
  case datetime: DateTime =>
    JString(datetime.toLocalDateTime.toString)
}))