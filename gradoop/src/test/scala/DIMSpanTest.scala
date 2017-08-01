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
package au.csiro.data61.gradoop

import java.io.{File, FileInputStream}
import java.nio.file.{Path, Paths}

import org.junit.runner.RunWith
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import org.scalatest.junit.JUnitRunner

import language.postfixOps
import scala.util.{Failure, Success, Try}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import au.csiro.data61.types.Exceptions.ModelerException
import au.csiro.data61.types._
import org.gradoop.flink.model.impl.LogicalGraph

import scala.collection.JavaConverters._


/**
  * Holds the implicit modeller objects for the Json4s Serializers.
  *
  * This is actually needed only for tests
  */
trait ModelerJsonFormats {
  implicit def json4sFormats: Formats =
    org.json4s.DefaultFormats +
      JodaTimeSerializer +
      SsdNodeSerializer +
      HelperLinkSerializer +
      SemanticModelSerializer +
      SsdMappingSerializer
}

/**
  * Created by natalia on 23/06/17.
  */
@RunWith(classOf[JUnitRunner])
class DIMSpanTest extends FunSuite with ModelerJsonFormats with BeforeAndAfterEach {

  lazy val ssdDir = getClass.getResource("/ssd").getPath

  lazy val exampleSM: String = Paths.get(ssdDir,"semantic_model_example.json") toString
  lazy val museumSSD: String = Paths.get(ssdDir, "s28-wildlife-art.ssd") toString
  lazy val smallSM: String = Paths.get(ssdDir,"small_semantic_model.json") toString
  lazy val businessSSD: String = Paths.get(ssdDir,"businessInfo.ssd") toString
  lazy val citiesSSD: String = Paths.get(ssdDir,"getCities.ssd") toString
  lazy val alignJson: String = getClass.getResource("align_business_cities.json").getPath


  lazy val smallPieces = List("t # 0",
    "v 0 http://www.semanticweb.org/serene/report_example_ontology#Organization",
    "v 1 Organization.name",
    "e 0 1 http://www.semanticweb.org/serene/report_example_ontology#name")

  def removeAll(path: Path): Unit = {
    def getRecursively(f: Path): Seq[Path] =
      f.toFile.listFiles
        .filter(_.isDirectory)
        .flatMap { x => getRecursively(x.toPath) } ++
        f.toFile.listFiles.map(_.toPath)
    getRecursively(path).foreach { f =>
      if (!f.toFile.delete) {throw ModelerException(s"Failed to delete ${f.toString}")}
    }
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

  override def afterEach(): Unit = {
    // clean the temporary folder
    removeAll(Paths.get("/tmp", "gradoop"))
  }

  def readSemModel(smPath: String): SemanticModel = {
    Try {
      val stream = new FileInputStream(Paths.get(smPath).toFile)
      parse(stream).extract[SemanticModel]
    } match {
      case Success(semModel) =>
        semModel
      case Failure(err) =>
        print(err.getMessage)
        fail(err.getMessage)
    }
  }

  test("Convert small semantic model to TLF") {
    val sm: SemanticModel = readSemModel(smallSM)
    assert(sm.getNodes.size === 2)
    assert(sm.getLinks.size === 1)

    val dimSpanSmall = DIMSpanTLFSourceWrapper(List(sm))
    dimSpanSmall.toTLF(reLabel = false) match {
      case Success(tlfString) =>
        assert(tlfString === smallPieces.mkString("\n"))
      case Failure(err) =>
        fail(err.getMessage)
    }
  }

  test("Convert small semantic model to Logical Graph with label re-index and original props") {
    val sm: SemanticModel = readSemModel(smallSM)
    assert(sm.getNodes.size === 2)
    assert(sm.getLinks.size === 1)

    val dimSpanSmall = DIMSpanTLFSourceWrapper(List(sm))
    dimSpanSmall.getLogicalGraph(sm, None, None, reLabel = true, createProps = true) match {
      case Success(lg) =>
        assert(lg.getVertices.count() === 2)
        assert(lg.getEdges.count() === 1)
      case Failure(err) =>
        fail(err.getMessage)
    }
  }

  test("Convert small semantic model to Logical Graph with original labels and original props") {
    val sm: SemanticModel = readSemModel(smallSM)
    assert(sm.getNodes.size === 2)
    assert(sm.getLinks.size === 1)

    val dimSpanSmall = DIMSpanTLFSourceWrapper(List(sm))
    dimSpanSmall.getLogicalGraph(sm, None, None, reLabel = false, createProps = true) match {
      case Success(lg) =>
        assert(lg.getVertices.count() === 2)
        assert(lg.getEdges.count() === 1)
      case Failure(err) =>
        fail(err)
    }
  }

  test("Convert small semantic model to Logical Graph with label re-index and no props") {
    val sm: SemanticModel = readSemModel(smallSM)
    assert(sm.getNodes.size === 2)
    assert(sm.getLinks.size === 1)

    val dimSpanSmall = DIMSpanTLFSourceWrapper(List(sm))
    dimSpanSmall.getLogicalGraph(sm, None, None, reLabel = true, createProps = false) match {
      case Success(lg) =>
        assert(lg.getVertices.count() === 2)
        assert(lg.getEdges.count() === 1)
      case Failure(err) =>
        fail(err.getMessage)
    }
  }

  test("Convert small semantic model to Logical Graph with original labels and no props") {
    val sm: SemanticModel = readSemModel(smallSM)
    assert(sm.getNodes.size === 2)
    assert(sm.getLinks.size === 1)

    val dimSpanSmall = DIMSpanTLFSourceWrapper(List(sm))
    dimSpanSmall.getLogicalGraph(sm, None, None, reLabel = false, createProps = false) match {
      case Success(lg) =>
        assert(lg.getVertices.count() === 2)
        assert(lg.getEdges.count() === 1)
      case Failure(err) =>
        fail(err)
    }
  }

  test("Convert two small semantic models to TLF") {
    val sm: SemanticModel = readSemModel(smallSM)

    DIMSpanTLFSourceWrapper(List(sm, sm)).toTLF(reLabel = false) match {
      case Success(tlfString) =>
        val pieces = "t # 1" :: smallPieces.drop(1)
        assert(tlfString === smallPieces.mkString("\n") + "\n" + pieces.mkString("\n"))
      case Failure(err) =>
        fail(err.getMessage)
    }
  }

  test("Convert two museum semantic models to TLF") {
    val sm: SemanticModel = readSSD(museumSSD).semanticModel.get

    DIMSpanTLFSourceWrapper(List(sm, sm)).toTLF(reLabel = true) match {
      case Success(tlfString) =>
        val pieces = "t # 1" :: smallPieces.drop(1)
        assert(tlfString === smallPieces.mkString("\n") + "\n" + pieces.mkString("\n"))
      case Failure(err) =>
        fail(err.getMessage)
    }
  }

  test("Convert two small semantic models to TLF and write the output to file") {
    val sm: SemanticModel = readSemModel(smallSM)

    val tmpOutput = Paths.get("/tmp", "gradoop", "tlf_example.tlf").toString
    DIMSpanTLFSourceWrapper(List(sm, sm)).toTLF(outputPath = Some(tmpOutput),
      reLabel = false) match {
      case Success(tlfString) =>
        val pieces = "t # 1" :: smallPieces.drop(1)
        assert(tlfString === smallPieces.mkString("\n") + "\n" + pieces.mkString("\n"))
      case Failure(err) =>
        fail(err.getMessage)
    }
  }

  test("Mine frequent patterns in small semantic model and save to file") {
    val sm: SemanticModel = readSemModel(smallSM)

    val tmpInput = Paths.get("/tmp", "gradoop", "small.tlf").toString
    val dimSpanSmall = DIMSpanTLFSourceWrapper(List(sm))

    val tmpOutput = Paths.get("/tmp", "gradoop", "small_pats.json").toString
    assert(dimSpanSmall.saveFrequentPatterns(tmpInput, tmpOutput, 0.5, false, true).isDefined)

  }

  test("Mine frequent patterns in small semantic model") {
    val sm: SemanticModel = readSemModel(smallSM)

    val tmpInput = Paths.get("/tmp", "gradoop", "small.tlf").toString
    val dimSpanSmall = DIMSpanTLFSourceWrapper(List(sm))

    val tmpOutput = Paths.get("/tmp", "gradoop", "small_pats.tlf").toString

    val tlfString = dimSpanSmall.toTLF(outputPath = Some(tmpInput))
    val optPatterns = dimSpanSmall.executeMiningGDL(tmpInput, tmpOutput, 0.5, false)

    println(optPatterns)
    assert(optPatterns.isSuccess)
    assert(optPatterns.get.count() === 1)

    dimSpanSmall.dfs.close()
  }

  test("Mine frequent patterns in example semantic model and save to file") {
    val sm: SemanticModel = readSemModel(exampleSM)

    val tmpInput = Paths.get("/tmp",  "gradoop", "example_sm.tlf").toString
    val dimSpanSmall = DIMSpanTLFSourceWrapper(List(sm))

    val tmpOutput = Paths.get("/tmp", "gradoop", "example_sm_pats.json").toString
    assert(dimSpanSmall.saveFrequentPatterns(tmpInput, tmpOutput, 0.5, false, true).isDefined)

  }

  test("Mine frequent patterns in example semantic model with max threshold") {
    val sm: SemanticModel = readSemModel(exampleSM)

    val tmpInput = Paths.get("/tmp", "gradoop", "example_sm.tlf").toString
    val dimSpanSmall = DIMSpanTLFSourceWrapper(List(sm))

    val tmpOutput = Paths.get("/tmp", "gradoop", "example_pats.json").toString

    val tlfString = dimSpanSmall.toTLF(outputPath = Some(tmpInput))
    val optPatterns = dimSpanSmall.executeMiningGDL(tmpInput, tmpOutput, 1, false)

    assert(optPatterns.isSuccess)
    assert(optPatterns.get.count() === 48)

    dimSpanSmall.dfs.close()
  }

  test("Mine frequent patterns among two models") {
    val tmpInput = Paths.get("/tmp", "gradoop", "two_sm.tlf").toString
    val dimSpanSmall = DIMSpanTLFSourceWrapper(List(readSemModel(exampleSM), readSemModel(smallSM)))
    val tmpOutput = Paths.get("/tmp", "gradoop", "two_pats.json").toString

    val tlfString = dimSpanSmall.toTLF(outputPath = Some(tmpInput))
    val optPatterns = dimSpanSmall.executeMiningGDL(tmpInput, tmpOutput, 1.0, false)

    assert(optPatterns.isSuccess)
    assert(optPatterns.get.count() === 1)

    dimSpanSmall.dfs.close()
  }


  test("Find embeddings for frequent patterns from a small semantic model in the alignment graph") {
    // getting patterns
    val sm1: SemanticModel = readSSD(citiesSSD).semanticModel.get
    val sm2: SemanticModel = readSSD(businessSSD).semanticModel.get

    val tmpInput = Paths.get("/tmp", "gradoop", "business_cities.tlf").toString
    val dimSpan = DIMSpanTLFSourceWrapper(List(sm1, sm2))

    val tmpOutput = Paths.get("/tmp", "gradoop", "business_cities_pats.tlf").toString

    val tlfString = dimSpan.toTLF(outputPath = Some(tmpInput))
    val optPatterns = dimSpan.executeMiningGDL(tmpInput, tmpOutput, 1.0, directed=true)

    assert(optPatterns.isSuccess)
    assert(optPatterns.get.count() === 6)
    println("****Patterns*****")
    optPatterns.get.print()
    println("*********")

    // finding embeddings
    val writeName = "/tmp/gradoop/museum2_embeds_normal/"
    dimSpan.findEmbeddings(alignJson, optPatterns.get, writeFile = writeName)

    dimSpan.dfs.close()
  }

  test("getGraphCollection for 2 small semantic models") {
    val sm: SemanticModel = readSemModel(smallSM)

    val dimSpan = DIMSpanTLFSourceWrapper(List(sm, sm))

    println("*********Lookup***")
    println(dimSpan.LabelLookup)
    println("*********")

    val collection = dimSpan.getGraphCollection()

    assert(collection.isSuccess)
    assert(collection.get.getGraphHeads.count === 2)
    assert(collection.get.getVertices.count === 2)
    assert(collection.get.getVertices.collect.asScala.forall(_.getGraphCount == 2))
    assert(collection.get.getEdges.count === 1)

    dimSpan.dfs.close()
  }

  test("Mine patterns in 2 small semantic models") {
    val sm: SemanticModel = readSemModel(smallSM)

    val dimSpan = DIMSpanTLFSourceWrapper(List(sm, sm))

    println("*********Lookup***")
    println(dimSpan.LabelLookup)
    println("*********")

    val tmpOutput = Paths.get("/tmp", "gradoop", "small_pats.tlf").toString
    val optPatterns = dimSpan.mineGDL(tmpOutput, 0.5, directed=true)

    assert(optPatterns.isSuccess)
    assert(optPatterns.get.count === 1)

    dimSpan.dfs.close()
  }

  test("Mine frequent patterns from museum domain by including everything") {
    // museum ssds
    lazy val ssdList: List[String] = new File(getClass.getResource("/museum").getPath)
      .listFiles.map(_.getAbsolutePath).filter(_.endsWith(".ssd")).toList
    println("SSDS")
    ssdList.foreach(println)
    println("========")

    // alignment graph
    lazy val museumAlign: String = Paths.get(getClass.getResource("/museum").getPath, "alignment.json").toString

    // getting patterns
    val semModels: List[SemanticModel] = ssdList.map(sm => readSSD(sm).semanticModel.get)

    val dimSpan = DIMSpanTLFSourceWrapper(semModels)

    val tmpOutput = Paths.get("/tmp", "gradoop", "museum_sample_pats.tlf").toString

    val optPatterns = dimSpan.mineGDL(tmpOutput, 1.0, directed=true, skipData = false, skipUnknown = false)

    assert(optPatterns.isSuccess)
    println(s"Number of found pattern: ${optPatterns.get.count}")
    assert(optPatterns.get.count === 10)

    dimSpan.dfs.close()
  }

  test("Mine frequent patterns from museum domain by skipping data and unknown nodes") {
    // museum ssds
    lazy val ssdList: List[String] = new File(getClass.getResource("/museum").getPath)
      .listFiles.map(_.getAbsolutePath).filter(_.endsWith(".ssd")).toList
    println("SSDS")
    ssdList.foreach(println)
    println("========")

    // alignment graph
    lazy val museumAlign: String = Paths.get(getClass.getResource("/museum").getPath, "alignment.json").toString

    // getting patterns
    val semModels: List[SemanticModel] = ssdList.map(sm => readSSD(sm).semanticModel.get)

    val dimSpan = DIMSpanTLFSourceWrapper(semModels)

    val tmpOutput = Paths.get("/tmp", "gradoop", "museum_sample_pats.json").toString

    val optPatterns = dimSpan.mineGDL(tmpOutput, 0.7, directed=true, skipData = true, skipUnknown = true)

    assert(optPatterns.isSuccess)
    println(s"Number of found pattern: ${optPatterns.get.count}")
    assert(optPatterns.get.count === 1)

    dimSpan.dfs.close()
  }

  test("Mine frequent patterns from museum domain by skipping only unknown nodes") {
    // museum ssds
    lazy val ssdList: List[String] = new File(getClass.getResource("/museum").getPath)
      .listFiles.map(_.getAbsolutePath).filter(_.endsWith(".ssd")).toList
    println("SSDS")
    ssdList.foreach(println)
    println("========")

    // alignment graph
    lazy val museumAlign: String = Paths.get(getClass.getResource("/museum").getPath, "alignment.json").toString

    // getting patterns
    val semModels: List[SemanticModel] = ssdList.map(sm => readSSD(sm).semanticModel.get)

    val dimSpan = DIMSpanTLFSourceWrapper(semModels)

    val tmpOutput = Paths.get("/tmp", "gradoop", "museum_sample_pats.tlf").toString

    val optPatterns = dimSpan.mineGDL(tmpOutput, 0.7, directed=true, skipData = false, skipUnknown = true)

    assert(optPatterns.isSuccess)
    println(s"Number of found pattern: ${optPatterns.get.count}")
    assert(optPatterns.get.count === 48)

    dimSpan.dfs.close()
  }

  test("smToLogicalGraph for 2 small semantic models") {
    val sm: SemanticModel = readSemModel(smallSM)

    val dimSpan = DIMSpanTLFSourceWrapper(List(sm, sm))

    println("*********Lookup***")
    println(dimSpan.LabelLookup)
    println("*********")

    val collection = dimSpan.smToLogicalGraph(reLabel = true)

    assert(collection.isSuccess)
    assert(collection.get.getGraphHead.count === 2)
    assert(collection.get.getVertices.count === 2)
    assert(collection.get.getEdges.count === 1)
    assert(collection.get.getVertices.collect.asScala.forall(_.getGraphCount == 2))
    assert(collection.get.getEdges.collect.asScala.forall(_.getGraphCount == 2))

    dimSpan.dfs.close()
  }

  test("Converting museum semantic models to logical graph with everything included") {
    // museum ssds
    lazy val ssdList: List[String] = new File(getClass.getResource("/museum").getPath)
      .listFiles.map(_.getAbsolutePath).filter(_.endsWith(".ssd")).toList
    println("SSDS")
    ssdList.foreach(println)
    println("========")

    // getting patterns
    val semModels: List[SemanticModel] = ssdList.map(sm => readSSD(sm).semanticModel.get)

    val dimSpan = DIMSpanTLFSourceWrapper(semModels)

    println("*********Lookup***")
    dimSpan.LabelLookup.foreach(println)
    println("*********")

    val collection = dimSpan.smToLogicalGraph(reLabel=true, skipData=false, skipUnknown=false)

    assert(collection.isSuccess)
    assert(collection.get.getGraphHead.count === semModels.size)
    assert(collection.get.getVertices.count === 98)
    assert(collection.get.getEdges.count === 133)

    dimSpan.dfs.close()
  }

  test("Converting museum semantic models to logical graph by skipping all and unknown") {
    // museum ssds
    lazy val ssdList: List[String] = new File(getClass.getResource("/museum").getPath)
      .listFiles.map(_.getAbsolutePath).filter(_.endsWith(".ssd")).toList
    println("SSDS")
    ssdList.foreach(println)
    println("========")

    // getting patterns
    val semModels: List[SemanticModel] = ssdList.map(sm => readSSD(sm).semanticModel.get)

    val dimSpan = DIMSpanTLFSourceWrapper(semModels)

    println("*********Lookup***")
    dimSpan.LabelLookup.foreach(println)
    println("*********")

    val collection = dimSpan.smToLogicalGraph(reLabel=false, skipData=true, skipUnknown=true)

    assert(collection.isSuccess)
    assert(collection.get.getGraphHead.count === semModels.size)
    assert(collection.get.getVertices.count === 15)
    assert(collection.get.getEdges.count === 20)

    dimSpan.dfs.close()
  }

//  test("Find embeddings for frequent patterns from museum domain in the alignment graph by including everything") {
//    // museum ssds
//    lazy val ssdList: List[String] = new File(getClass.getResource("/museum").getPath)
//      .listFiles.map(_.getAbsolutePath).filter(_.endsWith(".ssd")).toList
//    println("SSDS")
//    ssdList.foreach(println)
//    println("========")
//
//    // alignment graph
//    lazy val museumAlign: String = Paths.get(getClass.getResource("/museum").getPath, "alignment.json").toString
//
//    // getting patterns
//    val semModels: List[SemanticModel] = ssdList.map(sm => readSSD(sm).semanticModel.get)
//
//    val dimSpan = DIMSpanTLFSourceWrapper(semModels)
//
//    val tmpOutput = Paths.get("/tmp", "gradoop", "museum_sample_pats.json").toString
//
//    val optPatterns = dimSpan.mineGDL(tmpOutput, 1.0, directed=true, skipData = false, skipUnknown = false)
//
//    assert(optPatterns.isSuccess)
//    println(s"Number of found pattern: ${optPatterns.get.count}")
//    assert(optPatterns.get.count() === 10)
//
//    // finding embeddings
//    val writeName = "/tmp/gradoop/museum2_embeds_normal/"
//    val embeds = dimSpan.findEmbeddings(museumAlign, optPatterns.get,
//      linkSize = SizeRange(lower=Some(1)), writeName)
//
//    assert(embeds.getGraphHeads.count === 226)
//
//    println("****Patterns*****")
//    optPatterns.get.print()
//    println("*********")
//
//    println("*********Lookup***")
//    println(dimSpan.LabelLookup)
//    println("*********")
//
//
//    dimSpan.dfs.close()
//  }

//  test("Find embeddings for frequent patterns from museum domain in the alignment graph by skipping only unknown") {
//    // museum ssds
//    lazy val ssdList: List[String] = new File(getClass.getResource("/museum").getPath)
//      .listFiles.map(_.getAbsolutePath).filter(_.endsWith(".ssd")).toList
//    println("SSDS")
//    ssdList.foreach(println)
//    println("========")
//
//    // alignment graph
//    lazy val museumAlign: String = Paths.get(getClass.getResource("/museum").getPath, "alignment.json").toString
//
//    // getting patterns
//    val semModels: List[SemanticModel] = ssdList.map(sm => readSSD(sm).semanticModel.get)
//
//    val dimSpan = DIMSpanTLFSourceWrapper(semModels)
//
//    val tmpOutput = Paths.get("/tmp", "gradoop", "museum_data_pats.tlf").toString
//
//    val sup = 2.0 / semModels.size
//    println(s"Mining support: $sup")
//    val optPatterns = dimSpan.mineGDL(tmpOutput, 0.4, directed=true, skipData = false, skipUnknown = true)
//
//    assert(optPatterns.isSuccess)
//    println(s"Number of found pattern: ${optPatterns.get.count}")
////    assert(optPatterns.get.count() === 302)
//
//    // finding embeddings
//    val writeName = "/tmp/gradoop/museum2_embeds_normal/"
//    val embeds = dimSpan.findEmbeddings(museumAlign, optPatterns.get,
//      linkSize = SizeRange(lower=Some(1), upper = Some(6)),
//      writeName)
//
//    assert(embeds.getGraphHeads.count === 302)
//
//    println("****Patterns*****")
//    optPatterns.get.print()
//    println("*********")
//
//    println("*********Lookup***")
//    println(dimSpan.LabelLookup)
//    println("*********")
//
//
//    dimSpan.dfs.close()
//  }

  test("Find embeddings for frequent patterns from museum domain in the alignment graph by skipping unknown and all") {
    // museum ssds
    lazy val ssdList: List[String] = new File(getClass.getResource("/museum2").getPath)
      .listFiles.map(_.getAbsolutePath).filter(_.endsWith(".ssd")).toList
    println("SSDS")
    ssdList.foreach(println)
    println("========")

    val cor = List(227799468, 332540349, 340291617, 460525736, 558197382,
      606940669, 694285545, 727946556, 1454202572, 1570941292, 1799544871,
      1870411464, 1918293863, 2094713601)
    assert(ssdList.size === cor.size)

    // alignment graph
    lazy val museumAlign: String = Paths.get(getClass.getResource("/museum2").getPath, "alignment.json").toString

    // getting patterns
    val semModels: List[SemanticModel] = ssdList.map(sm => readSSD(sm).semanticModel.get)

    val dimSpan = DIMSpanTLFSourceWrapper(semModels)

    val tmpOutput = Paths.get("/tmp", "gradoop", "museum2_normal_pats.tlf").toString

    val sup = 2.0 / semModels.size
    println(s"Mining support: $sup")
    val optPatterns = dimSpan.mineGDL(tmpOutput, sup, directed=true, skipData = true, skipUnknown = true)

    assert(optPatterns.isSuccess)
    println(s"Number of found pattern: ${optPatterns.get.count}")
    assert(optPatterns.get.count() === 38)

    // finding embeddings
    val writeName = "/tmp/gradoop/museum2_embeds_normal/"
    val embeds = dimSpan.findEmbeddings(museumAlign,
      optPatterns.get,
      linkSize = SizeRange(lower=Some(1), upper=Some(6)),
      writeName)

    assert(embeds.getGraphHeads.count === 234)
    dimSpan.dfs.close()
  }

  test("Converting alignment graph for museums domain") {
    // museum ssds
    lazy val ssdList: List[String] = new File(getClass.getResource("/museum").getPath)
      .listFiles.map(_.getAbsolutePath).filter(_.endsWith(".ssd")).toList
    println("SSDS")
    ssdList.foreach(println)
    println("========")

    // getting patterns
    val semModels: List[SemanticModel] = ssdList.map(sm => readSSD(sm).semanticModel.get)

    val dimSpan = DIMSpanTLFSourceWrapper(semModels)

    // alignment graph
    lazy val museumAlign: String = Paths.get(getClass.getResource("/museum").getPath, "alignment.json").toString
    val (alignGraph: SemanticModel, nodeIdMap: Map[String, Int], linkIdMap: Map[String, Int]) =
      KarmaTypes.readAlignmentGraph(museumAlign)

    assert(alignGraph.getNodes.size === 98)
    assert(alignGraph.getHelperLinks.size === 143)
    assert(linkIdMap.size === alignGraph.getHelperLinks.size)
    assert(nodeIdMap.size === alignGraph.getNodes.size)

    val graph: Try[LogicalGraph] = dimSpan
      .getLogicalGraph(alignGraph,
        Some(nodeIdMap.map { case (a,b) => (b, a)}.toMap),
        Some(linkIdMap.map { case (a,b) => (b, a)}.toMap)
      )

    assert(graph.isSuccess)
    assert(graph.get.getGraphHead.count === 1)
    assert(graph.get.getVertices.count === alignGraph.getNodes.size)
    assert(graph.get.getEdges.count === alignGraph.getHelperLinks.size)

    dimSpan.dfs.close()
  }
}

