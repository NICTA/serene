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

import java.io
import java.io.{File, FileInputStream, PrintWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import au.csiro.data61.types.ColumnTypes._
import org.jgrapht.graph.DirectedWeightedMultigraph
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.scalatest.junit.JUnitRunner
import com.typesafe.scalalogging.LazyLogging

import language.postfixOps
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}
import edu.isi.karma.modeling.alignment.learner.ModelReader
import edu.isi.karma.modeling.research.Params
import edu.isi.karma.modeling.alignment.{SemanticModel => KarmaSSD}
import au.csiro.data61.types._
import au.csiro.data61.modeler.karma.{KarmaBuildAlignmentGraph, KarmaParams, KarmaSuggestModel}
import au.csiro.data61.types.SSDTypes._

/**
  * Created by natalia on 14/11/16.
  */
@RunWith(classOf[JUnitRunner])
class MuseumSpec extends FunSuite with ModelerJsonFormats with BeforeAndAfterEach with LazyLogging{

  val ssdDir = getClass.getResource("/ssd").getPath
  val karmaDir = getClass.getResource("/karma").getPath
  val exampleSSD: String = Paths.get(ssdDir,"businessInfo.ssd") toString
  val emptySSD: String = Paths.get(ssdDir,"empty_model_2.ssd") toString
  val partialSSD: String = Paths.get(ssdDir,"partial_model.ssd") toString
  val veryPartialSSD: String = Paths.get(ssdDir,"partial_model2.ssd") toString
  val emptyCitiesSSD: String = Paths.get(ssdDir,"empty_getCities.ssd") toString

  test("Museum dataset crm read in"){
    val jsonList: Array[String] = Paths.get(karmaDir, "museum", "museum-29-crm")
      .toFile.listFiles
      .filter(_.toString.endsWith(Params.MODEL_MAIN_FILE_EXT))
      .map(_.getAbsolutePath)
    println(s"jsonList: $jsonList")


    val semanticModels: Array[KarmaSSD]  =
      jsonList.map {
        fileName =>
          KarmaSSD.readJson(fileName)
      }

    val res:Array[(String, String, String, String, String)] = semanticModels.flatMap {
      ssd => {
        val dsName = ssd.getName
        val columns: List[(String, String, String, String, String)] = ssd.getColumnNodes
          .asScala.toList
          .map {
            columnN =>
              val colName: String = columnN.getColumnName
              val hNodeId: String = columnN.getHNodeId
              val semTypes: List[(String,String)] = columnN.getUserSemanticTypes.asScala
                .toList.map(st => (st.getDomain.getUri, st.getType.getUri))
              val semType: (String,String) = semTypes.isEmpty match {
                case true => ("","")
                case false => semTypes(0)
              }
              (dsName, colName, hNodeId, semType._1, semType._2)
          }
          columns
      }
    }

    println(s"${semanticModels.length} Read")
    println(s"Result: ${res}")

    var out = new PrintWriter(new File(Paths.get(karmaDir, "museum", "museum-29-crm.csv").toString))
    out.println("datasetName,columnHeader,hNodeId,domainUri,typeUri")
    res.foreach({tup => out.println(tup.productIterator.mkString(","))})
    out.close()

    succeed
  }

  test("Museum dataset edm read in"){
    val jsonList: Array[String] = Paths.get(karmaDir, "museum", "museum-29-edm")
      .toFile.listFiles
      .filter(_.toString.endsWith(Params.MODEL_MAIN_FILE_EXT))
      .map(_.getAbsolutePath)
    println(s"jsonList: $jsonList")


    val semanticModels: Array[KarmaSSD]  =
      jsonList.map {
        fileName =>
          KarmaSSD.readJson(fileName)
      }

    val res:Array[(String, String, String, String, String)] = semanticModels.flatMap {
      ssd => {
        val dsName = ssd.getName
        val columns: List[(String, String, String, String, String)] = ssd.getColumnNodes
          .asScala.toList
          .map {
            columnN =>
              val colName: String = columnN.getColumnName
              val hNodeId: String = columnN.getHNodeId
              val semTypes: List[(String,String)] = columnN.getUserSemanticTypes.asScala
                .toList.map(st => (st.getDomain.getUri, st.getType.getUri))
              val semType: (String,String) = semTypes.isEmpty match {
                case true => ("","")
                case false => semTypes(0)
              }
              (dsName, colName, hNodeId, semType._1, semType._2)
          }
        columns
      }
    }

    println(s"${semanticModels.length} Read")
    println(s"Result: ${res}")

    var out = new PrintWriter(new File(Paths.get(karmaDir, "museum", "museum-29-edm.csv").toString))
    out.println("datasetName,columnHeader,hNodeId,domainUri,typeUri")
    res.foreach({tup => out.println(tup.productIterator.mkString(","))})
    out.close()

    succeed
  }
}
