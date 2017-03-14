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

import au.csiro.data61.core.api.{EvaluationRequest, SsdRequest}
import au.csiro.data61.core.storage.JsonFormats
import au.csiro.data61.modeler.EvaluationResult
import au.csiro.data61.types.Ssd
import com.twitter.finagle.http.{Response, Request, Status}
import com.twitter.finagle.http.Method.Post
import com.twitter.io.Buf.ByteArray
import com.twitter.util.Await

import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization.write

import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.language.postfixOps
import scala.util.{Failure, Success, Try}


/**
  * Tests for evaluation of semantic modeling
  */
@RunWith(classOf[JUnitRunner])
class TestAPISpec  extends FunSuite with JsonFormats {

  import au.csiro.data61.core.api.TestAPI._

  val ssdDir = getClass.getResource("/ssd").getPath

  def toSsdRequest(ssd: Ssd): SsdRequest = {
    SsdRequest(name = ssd.name,
      ontologies = ssd.ontology,
      mappings = ssd.mappings,
      semanticModel = ssd.semanticModel
    )
  }

  def readSSD(ssdPath: String): Ssd = {
    Try {
      val stream = new FileInputStream(Paths.get(ssdPath).toFile)
      parse(stream).extract[Ssd]
    } match {
      case Success(ssd) =>
        ssd
      case Failure(err) =>
        fail(err.getMessage)
    }
  }

  val partialSSD: Ssd = readSSD(Paths.get(ssdDir,"partial_model.ssd").toString)
  val veryPartialSSD: Ssd = readSSD(Paths.get(ssdDir,"partial_model2.ssd").toString)
  val businessSSD: Ssd = readSSD(Paths.get(ssdDir,"businessInfo.ssd").toString)
  val citiesSSD: Ssd = readSSD(Paths.get(ssdDir,"getCities.ssd").toString)
  val predCitiesSSD: Ssd = readSSD(Paths.get(ssdDir,"predicted_cities.ssd").toString)
  val predCitiesSSD2: Ssd = readSSD(Paths.get(ssdDir,"predicted_cities2.ssd").toString)
  val emptySSD: Ssd = readSSD(Paths.get(ssdDir,"empty_model.ssd").toString)

  def createEvaluationRequest(predictedSsd: Ssd,
                              correctSsd: Ssd,
                              ignoreSemanticTypes: Boolean = true,
                              ignoreColumnNodes: Boolean = false)(implicit server: TestServer)
  : Response = {

    val evalRequest = EvaluationRequest(toSsdRequest(predictedSsd),
      toSsdRequest(correctSsd),
      ignoreSemanticTypes,
      ignoreColumnNodes)

    val request = Request(Post, s"/$APIVersion/evaluate")
    request.content = ByteArray(write(evalRequest).getBytes: _*)
    request.contentType = "application/json"

    val response: Response = Await.result(server.client(request))
    response
  }

  test("Correct business should have highest evaluation scores")( new TestServer {
    try {
      val response = createEvaluationRequest(businessSSD, businessSSD, false, false)
      assert(response.status === Status.Ok)

      val evalRes = parse(response.contentString).extract[EvaluationResult]

      // we do not check the correctness of semantic types, but just of the links!
      assert(evalRes.jaccard == 1.0)
      assert(evalRes.precision == 1.0)
      assert(evalRes.recall == 1.0)
    } finally assertClose()
  })

  test("Correct cities should have highest evaluation scores")( new TestServer {

    try{
      val response = createEvaluationRequest(citiesSSD, citiesSSD, true, true)
      assert(response.status === Status.Ok)

      val evalRes = parse(response.contentString).extract[EvaluationResult]

      // we do not check the correctness of semantic types, but just of the links!
      assert(evalRes.jaccard == 1.0)
      assert(evalRes.precision == 1.0)
      assert(evalRes.recall == 1.0)
    } finally assertClose()

  })

  test("Evaluation for empty ssd fails")( new TestServer {
    try {
      val response = createEvaluationRequest(emptySSD, businessSSD, true, false)
      assert(response.status === Status.InternalServerError)
    } finally assertClose()
  })

  test("Evaluation for partial businessInfo should be non zero if we consider semantictypes")( new TestServer {
    try {
      val response = createEvaluationRequest(partialSSD, businessSSD, false, true)
      assert(response.status === Status.Ok)

      val evalRes = parse(response.contentString).extract[EvaluationResult]
      println(evalRes)

      assert(evalRes.jaccard === 0.57)
      assert(evalRes.precision === 1.0)
      assert(evalRes.recall === 0.57)
    } finally assertClose()
  })

  test("Evaluation for partial businessInfo should be 0 if we do not consider semantic types")( new TestServer {
    try {
      val response = createEvaluationRequest(partialSSD, businessSSD, true, true)
      assert(response.status === Status.Ok)

      val evalRes = parse(response.contentString).extract[EvaluationResult]
      println(evalRes)

      assert(evalRes.jaccard === 0)
      assert(evalRes.precision === 0)
      assert(evalRes.recall === 0)
    } finally assertClose()
  })

  test("Evaluation for predicted cities")( new TestServer {
    try{
      val response = createEvaluationRequest(predCitiesSSD, citiesSSD, true, true)
      assert(response.status === Status.Ok)

      val evalRes = parse(response.contentString).extract[EvaluationResult]

      println(evalRes)

      assert(evalRes.jaccard === 0.5)
      assert(evalRes.precision === 0.5)
      assert(evalRes.recall === 1.0)
    } finally assertClose()
  })

  test("Evaluation for another predicted cities")( new TestServer {
    try{
      val response = createEvaluationRequest(predCitiesSSD2, citiesSSD, true, false)
      assert(response.status === Status.Ok)

      val evalRes = parse(response.contentString).extract[EvaluationResult]

      println(evalRes)

      assert(evalRes.jaccard === 1.0)
      assert(evalRes.precision === 1.0)
      assert(evalRes.recall === 1.0)
    } finally assertClose()
  })
}
