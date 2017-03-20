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

import java.io.{File, PrintWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import au.csiro.data61.types.{KarmaSemanticModel, Ssd}
import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.scalatest.junit.JUnitRunner
import com.typesafe.scalalogging.LazyLogging

import language.postfixOps
import scala.collection.JavaConverters._
import edu.isi.karma.modeling.research.Params
import edu.isi.karma.modeling.alignment.{SemanticModel => KarmaSSD}
import org.json4s.Extraction
import org.json4s.jackson.JsonMethods._

import scala.util.{Failure, Random, Success, Try}

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

  def genID: Int = Random.nextInt(Integer.MAX_VALUE)

  def writeToFile(dir: Path, ssd: Ssd): Unit = {
    val str = compact(Extraction.decompose(ssd))

    // ensure that the directories exist...
    if (!dir.toFile.exists) dir.toFile.mkdirs

    val outputPath = Paths.get(dir.toString, ssd.name + ".ssd")
    // write the object to the file system
    println(s"Writing to $outputPath")
    Files.write(
      outputPath,
      str.getBytes(StandardCharsets.UTF_8)
    )
  }

  test("Museum dataset edm models conversions"){
    val jsonList: Array[String] = Paths.get(karmaDir, "museum", "museum-29-edm")
      .toFile.listFiles
      .filter(_.toString.endsWith(Params.MODEL_MAIN_FILE_EXT))
      .map(_.getAbsolutePath)

    val semanticModels: Array[KarmaSSD]  =
      jsonList.map {
        fileName =>
          KarmaSSD.readJson(fileName)
      }
    assert(semanticModels.length === 29)

    // read in ontologies
    val ontologyNames = Paths.get(karmaDir, "museum", "museum-29-edm", "preloaded-ontologies")
      .toFile.listFiles.map(_.getName).zipWithIndex.map(x => (x._2,x._1)).toMap

//    val conv = KarmaSemanticModel(semanticModels.head).toSSD(newID = genID,
//      ontologies = ontologyNames.keys.toList)
//
//    assert(conv.semanticModel.get.getNodes
//      .filter(_.ssdLabel.labelType == "ClassNode")
//      .forall(_.ssdLabel.prefix.nonEmpty)
//    )
//
//    assert(conv.semanticModel.get.getHelperLinks.forall(_.prefix.nonEmpty))
//    assert(conv.semanticModel.get.getHelperLinks.map(_.prefix).distinct.size > 1)

    val convertedSemModels: Array[Ssd] = semanticModels.map(x =>
      KarmaSemanticModel(x).toSSD(newID = genID,
        ontologies = ontologyNames.keys.toList))

    assert(convertedSemModels.forall(_.semanticModel.isDefined))

    assert(convertedSemModels.flatMap(_.semanticModel)
      .flatMap(_.getNodes)
      .filter(_.ssdLabel.labelType == "ClassNode")
      .forall(_.ssdLabel.prefix.nonEmpty)
    )

    assert(convertedSemModels.flatMap(_.semanticModel)
      .flatMap(_.getHelperLinks).forall(_.prefix.nonEmpty))
    assert(convertedSemModels.flatMap(_.semanticModel)
      .flatMap(_.getHelperLinks).map(_.prefix).distinct.length > 1)

    val path = Paths.get(karmaDir, "museum", "museum-29-edm", "conversion")
    convertedSemModels.foreach(sm => writeToFile(path, sm))

    convertedSemModels.flatMap(_.semanticModel)
      .flatMap(_.getHelperLinks).map(_.lType).distinct.foreach(println)

  }
}
