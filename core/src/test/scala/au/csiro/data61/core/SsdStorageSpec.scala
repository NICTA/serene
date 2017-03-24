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

import java.io.FileInputStream
import java.nio.file.Paths

import au.csiro.data61.core.drivers.SsdInterface
import au.csiro.data61.core.storage.{JsonFormats, SsdStorage}
import au.csiro.data61.types._
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import language.postfixOps
import scala.util.{Failure, Success, Try}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import com.typesafe.scalalogging.LazyLogging

/**
  * Tests for the SSDStorage layer
  */

class SsdStorageSpec extends FunSuite with JsonFormats with BeforeAndAfterEach with LazyLogging{

  val ssdDir = getClass.getResource("/ssd").getPath
  val emptySsd: String = Paths.get(ssdDir, "empty_model.ssd") toString
  val exampleSsd: String = Paths.get(ssdDir, "businessInfo.ssd") toString

  override def beforeEach(): Unit = {
    SsdStorage.removeAll()
  }

  override def afterEach(): Unit = {
    SsdStorage.removeAll()
  }

  test("Successfully adding ssd to the storage") {
    assert(SsdStorage.keys.size === 0)
    Try {
      val stream = new FileInputStream(Paths.get(exampleSsd).toFile)
      parse(stream).extract[Ssd]
    } match {
      case Success(ssd) =>
        SsdStorage.add(ssd.id, ssd) match {
          case Some(added) =>
            assert(added === ssd.id)
          case _ =>
            fail("Not added!")
        }
        assert(SsdStorage.keys.size === 1)
        assert(SsdStorage.keys === List(ssd.id))
      case Failure(err) =>
        fail(err.getMessage)
    }
  }

  test("Incomplete ssd not added to the storage") {
    assert(SsdInterface.storageKeys.size === 0)
    Try {
      val stream = new FileInputStream(Paths.get(emptySsd).toFile)
      parse(stream).extract[Ssd]
    } match {
      case Success(ssd) =>
        assert(Try(SsdInterface.add(ssd)).isFailure)
        assert(SsdInterface.storageKeys.size === 0)
      case Failure(err) =>
        fail(err.getMessage)
    }
  }


}
