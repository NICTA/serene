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

import java.io.{InputStream, FileInputStream, File}
import java.nio.charset.StandardCharsets
import java.nio.file.{Path, Files, Paths, StandardCopyOption}

import au.csiro.data61.matcher.types.ModelTypes.{ModelID, Model}
import au.csiro.data61.matcher.types.{MatcherJsonFormats, DataSet, DataSetTypes}
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FileUtils

import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.util.{Success, Failure, Try}

import DataSetTypes._

import scala.language.postfixOps


/**
 * Holder for the rest of the code. Here we assume there is
 * some sort of database that holds the datasets. We require
 * methods that add a dataset, that can update a description,
 * or can update the type map.
 */
object StorageLayer extends LazyLogging with MatcherJsonFormats {

  /**
   * Adds a file resource into the storage system
   *
   * @param id The id for the storage element
   * @param stream The input stream
   * @return The path to the resource (if successful)
   */
  def addFile(id: DataSetID, stream: InputStream): Option[Path] = {

    val outputPath = Paths.get(DatasetDir, s"$id", s"$id.txt")

    Try {
      // ensure that the directories exist...
      outputPath.toFile.getParentFile.mkdirs

      // copy the file portion over into the output path
      Files.copy(
        stream,
        outputPath,
        StandardCopyOption.REPLACE_EXISTING
      )

      outputPath

    } toOption
  }

  /**
   * Add a new dataset to the storage layer
 *
   * @param id ID to give to the element
   * @param ds DataSet object
   * @return ID of the resource created (if any)
   */
  def addDataSet(id: DataSetID, ds: DataSet): Option[DataSetID] = {
    Try {
      synchronized {
        writeDataSetToFile(ds)
        datasets += (id -> ds)
      }
      id
    } toOption
  }

  /**
   * Add a new model to the storage layer
   *
   * @param id ID to give to the element
   * @param model Model object
   * @return ID of the resource created (if any)
   */
  def addModel(id: DataSetID, model: Model): Option[ModelID] = {
    Try {
      synchronized {
        writeModelToFile(model)
        models += (id -> model)
      }
      id
    } toOption
  }


  /**
   * Update the dataset id in the storage layer
 *
   * @param id ID to give to the element
   * @param ds DataSet object
   * @return ID of the resource created (if any)
   */
  def updateDataSet(id: DataSetID, ds: DataSet): Option[DataSetID] = {
    addDataSet(id, ds)
  }

  /**
   * Deletes the resource at the key `id`. This will also
   * delete any file resource at this key
   *
   * @param id Key for the dataset to be removed
   * @return Key of the removed dataset if successful
   */
  def removeDataSet(id: DataSetID): Option[DataSetID] = {
    datasets.get(id) match {
      case Some(ds) =>

        // delete directory - be careful
        val dir: File = ds.path.getParent.toFile

        synchronized {
          Try(FileUtils.deleteDirectory(dir)) match {
            case Failure(err) =>
              logger.error(s"Failed to delete directory: ${err.getMessage}")
              None
            case _ =>
              datasets -= id
              Some(id)
          }
        }
      case _ =>
        logger.error(s"Dataset not found: $id")
        None
    }
  }

  /**
   * Returns the dataset object at location id
 *
   * @param id The key for the dataset
   * @return Resource if available
   */
  def getDataSet(id: DataSetID): Option[DataSet] = {
    datasets.get(id)
  }

  def datasetKeys: List[DataSetID] = {
    datasets.keys.toList
  }

  def modelKeys: List[ModelID] = {
    models.keys.toList
  }

  /**
   * Internal functions...
   */

  protected val DatasetDir = Paths.get(Config.StoragePath, Config.DatasetDir).toString
  protected val ModelDir = Paths.get(Config.StoragePath, Config.ModelDir).toString

  protected var datasets = findDataSets.map(ds => ds.id -> ds).toMap
  protected var models = findModels.map(m => m.id -> m).toMap

  protected def toIntOption(s: String): Option[Int] = {
    Try(s.toInt).toOption
  }

  /**
   * Attempts to read all the datasets out from the storage dir
   *
   * @return
   */
  protected def findDataSets: List[DataSet] = {
    listDirectories(DatasetDir)
      .flatMap(toIntOption)
      .map(getDataSetPath)
      .flatMap(readDataSetFromFile)
  }

  /**
   * Attempts to read all the models out from the storage dir
   *
   * @return
   */
  protected def findModels: List[Model] = {
    listDirectories(ModelDir)
      .flatMap(toIntOption)
      .map(getModelPath)
      .flatMap(readModelFromFile)
  }

  /**
   * Helper function to list the directories
   *
   * @param rootDir The root directory from which to search
   * @return
   */
  protected def listDirectories(rootDir: String): List[String] = {
    Option(new File(rootDir) listFiles) match {
      case Some(fileList) =>
        fileList
          .filter(_.isDirectory)
          .map(_.getName)
          .toList
      case _ =>
        logger.error(s"Failed to open dir $rootDir")
        List.empty[String]
    }
  }

  /**
   * Returns the location of the JSON metadata file for DataSet id
   *
   * @param id The ID for the DataSet
   * @return
   */
  protected def getDataSetPath(id: DataSetID): Path = {
    Paths.get(DatasetDir, s"$id", s"$id.json")
  }

  /**
   * Returns the location of the JSON metadata file for Model id
   *
   * @param id The ID for the Model
   * @return
   */
  protected def getModelPath(id: ModelID): Path = {
    Paths.get(ModelDir, s"$id", s"$id.json")
  }

  /**
   * Attempts to read a JSON file and convert it into a DataSet
   * using the JSON reader.
   *
   * @param path Location of the JSON metadata file
   * @return
   */
  protected def readDataSetFromFile(path: Path): Option[DataSet] = {
    Try {
      val stream = new FileInputStream(path.toFile)
      parse(stream).extract[DataSet]
    } match {
      case Success(ds) =>
        Some(ds)
      case Failure(err) =>
        logger.warn(s"Failed to read file: ${err.getMessage}")
        None
    }
  }

  /**
   * Attempts to read a JSON file and convert it into a Model
   * using the JSON reader.
   *
   * @param path Location of the JSON metadata file
   * @return
   */
  protected def readModelFromFile(path: Path): Option[Model] = {
    Try {
      val stream = new FileInputStream(path.toFile)
      parse(stream).extract[Model]
    } match {
      case Success(model) =>
        Some(model)
      case Failure(err) =>
        logger.warn(s"Failed to read file: ${err.getMessage}")
        None
    }
  }

  /**
   * Writes the dataset ds to disk as a serialized json string
   * at a pre-defined location based on the id.
   *
   * @param ds The dataset to write to disk
   */
  protected def writeDataSetToFile(ds: DataSet): Unit = {

    val str = compact(Extraction.decompose(ds))

    val outputPath = getDataSetPath(ds.id)

    // ensure that the directories exist...
    val dir = outputPath.toFile.getParentFile

    if (!dir.exists) dir.mkdirs

    // write the dataset to the file system
    Files.write(
      outputPath,
      str.getBytes(StandardCharsets.UTF_8)
    )
  }

  /**
   * Writes the model to disk as a serialized json string
   * at a pre-defined location based on the id.
   *
   * @param model The model to write to disk
   */
  protected def writeModelToFile(model: Model): Unit = {

    val str = compact(Extraction.decompose(model))

    val outputPath = getModelPath(model.id)

    // ensure that the directories exist...
    val dir = outputPath.toFile.getParentFile

    if (!dir.exists) dir.mkdirs

    // write the dataset to the file system
    Files.write(
      outputPath,
      str.getBytes(StandardCharsets.UTF_8)
    )
  }
}