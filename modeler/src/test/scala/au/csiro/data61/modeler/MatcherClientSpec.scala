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
package au.csiro.data61.modeler

import java.io.File

import au.csiro.data61.matcher.api
import au.csiro.data61.matcher.types.{DataSet, MatcherJsonFormats}
import com.twitter.finagle.http.RequestBuilder
import com.twitter.finagle.http._
import com.twitter.io.Reader
import com.twitter.util.{Await, Closable}
import org.apache.commons.io.FileUtils
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.scalatest.junit.JUnitRunner

import language.postfixOps
import scala.util.{Failure, Random, Success, Try}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import MatcherClient._

/**
  * Tests for the Schema Matcher API
  */

@RunWith(classOf[JUnitRunner])
class MatcherClientSpec extends FunSuite with MatcherJsonFormats with BeforeAndAfterEach {

//  val matcher = MatcherClient
//
//  test("GET schema-matcher-api/v1.0/dataset responds Ok(200)") {
//    val response = matcher.get(s"/${Config.APIVersion}/dataset")
//    assert(response.contentType === Some(JsonHeader))
//    assert(response.status === Status.Ok)
//    assert(!response.contentString.isEmpty)
//  }
}
