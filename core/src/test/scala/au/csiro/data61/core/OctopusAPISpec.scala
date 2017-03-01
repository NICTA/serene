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
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import au.csiro.data61.core.api.OctopusRequest
import au.csiro.data61.core.storage.{JsonFormats, OctopusStorage, SsdStorage}
import au.csiro.data61.core.drivers.{MatcherInterface, OctopusInterface}
import au.csiro.data61.core.storage._
import au.csiro.data61.modeler.ModelerConfig
import au.csiro.data61.modeler.karma.{KarmaBuildAlignmentGraph, KarmaParams, KarmaSuggestModel}
import au.csiro.data61.types.ModelType.RANDOM_FOREST
import au.csiro.data61.types.ModelTypes.Model
import au.csiro.data61.types.SsdTypes.{Owl, OwlDocumentFormat}
import au.csiro.data61.types.SamplingStrategy.NO_RESAMPLING
import au.csiro.data61.types.Training.Status
import au.csiro.data61.types._
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FileUtils
import org.joda.time.DateTime
import org.json4s.Extraction
import org.json4s.jackson.JsonMethods._
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}



/**
  * Tests for the OctopusAPI endpoints
  */

class OctopusAPISpec extends FunSuite with JsonFormats with BeforeAndAfterEach with LazyLogging {
  override def afterEach(): Unit = {
    SsdStorage.removeAll()
    OctopusStorage.removeAll()
    DatasetStorage.removeAll()
    OwlStorage.removeAll()
    ModelStorage.removeAll()
  }

  val businessSsdID = 0
  val exampleOwlID = 1

  override def beforeEach(): Unit = {
    copySampleDatasets() // copy csv files for getCities and businessInfo
    SsdStorage.add(businessSSD.id, businessSSD) // add businessInfo ssd
    OwlStorage.add(exampleOwl.id, exampleOwl)  // add sample ontology
    // write owl file
    Try{
      val stream = new FileInputStream(exampleOntolPath.toFile)
      OwlStorage.writeOwlDocument(exampleOwlID, stream)
    }

  }

  val ssdDir = getClass.getResource("/ssd").getPath

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

  val exampleOntolPath = Paths.get(ssdDir,"dataintegration_report_ontology.owl")
  val exampleOwl = Owl(id = exampleOwlID, name = exampleOntolPath.toString, format = OwlDocumentFormat.DefaultOwl,
    description = "sample", dateCreated = DateTime.now, dateModified = DateTime.now)

  val partialSSD: Ssd = readSSD(Paths.get(ssdDir,"partial_model.ssd").toString)
  val veryPartialSSD: Ssd = readSSD(Paths.get(ssdDir,"partial_model2.ssd").toString)
  val emptyCitiesSSD: Ssd = readSSD(Paths.get(ssdDir,"empty_getCities.ssd").toString)
  val emptySSD: Ssd = readSSD(Paths.get(ssdDir,"empty_model.ssd").toString)
  val businessSSD: Ssd = readSSD(Paths.get(ssdDir,"businessInfo.ssd").toString)

  val defaultFeatures = FeaturesConfig(
    activeFeatures = Set("num-unique-vals", "prop-unique-vals", "prop-missing-vals",
      "ratio-alpha-chars", "prop-numerical-chars",
      "prop-whitespace-chars", "prop-entries-with-at-sign",
      "prop-entries-with-hyphen", "prop-entries-with-paren",
      "prop-entries-with-currency-symbol", "mean-commas-per-entry",
      "mean-forward-slashes-per-entry",
      "prop-range-format", "is-discrete", "entropy-for-discrete-values"),
    activeGroupFeatures = Set.empty[String],
    featureExtractorParams = Map()
  )

  val defaultOctopusRequest = OctopusRequest(
    name = None,
    description = Some("default octopus"),
    modelType = None,
    features = Some(defaultFeatures),
    resamplingStrategy = Some(NO_RESAMPLING),
    numBags = None,
    bagSize = None,
    ontologies = None,
    ssds = Some(List(businessSSD.id)),
    modelingProps = None)

  val blankOctopusRequest = OctopusRequest(None, None, None, None, None, None, None, None, None, None)

  val helperDir = getClass.getResource("/helper").getPath
  val sampleDsDir = getClass.getResource("/sample.datasets").getPath
  val datasetMap = Map("businessInfo" -> 767956483, "getCities" -> 696167703)
  val businessDsPath = Paths.get(sampleDsDir,
    datasetMap("businessInfo").toString, datasetMap("businessInfo").toString + ".json")
  val citiesDsPath = Paths.get(sampleDsDir,
    datasetMap("getCities").toString, datasetMap("getCities").toString + ".json")

  def copySampleDatasets(): Unit = {
    // copy sample dataset to Config.DatasetStorageDir
    if (!Paths.get(Serene.config.storageDirs.dataset).toFile.exists) { // create dataset storage dir
      Paths.get(Serene.config.storageDirs.dataset).toFile.mkdirs}
    val dsDir = Paths.get(sampleDsDir).toFile // directory to copy from
    FileUtils.copyDirectory(dsDir,                    // copy sample dataset
      Paths.get(Serene.config.storageDirs.dataset).toFile)

    // adding datasets explicitly to the storage
    val businessDS: DataSet = Try {
      val stream = new FileInputStream(businessDsPath.toFile)
      parse(stream).extract[DataSet]
    } match {
      case Success(ds) => ds
      case Failure(err) => fail(err.getMessage)
    }
    val citiesDS: DataSet = Try {
      val stream = new FileInputStream(businessDsPath.toFile)
      parse(stream).extract[DataSet]
    } match {
      case Success(ds) => ds
      case Failure(err) => fail(err.getMessage)
    }

    DatasetStorage.add(businessDS.id, businessDS)
    DatasetStorage.add(citiesDS.id, citiesDS)
  }
}
