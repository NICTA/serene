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

import java.io._
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardCopyOption}

import au.csiro.data61.matcher.types.ModelTypes.{Model, ModelID}
import au.csiro.data61.matcher.types._
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.{FileUtils, FilenameUtils}
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.util.{Failure, Success, Try}
import DataSetTypes._
import au.csiro.data61.matcher.api.parsers.FileStream

import scala.language.postfixOps
import com.nicta.dataint.matcher.serializable.SerializableMLibClassifier
import org.scalacheck.Prop.{False, True}

/**
 * Storage object that can store objects that are identifiable by a `Key`.
 * The child storage objects need to specify the root directory and an
 * `extract` method to convert a string to a `Value`.
 *
 * This maintains a cache map of the current state, and manages the hard
 * disk layer, storing objects in json format.
 *
 */
trait Storage[Key >: Int, Value <: Identifiable[Key]] extends LazyLogging with MatcherJsonFormats {

  def rootDir: String

  protected def extract(stream: FileInputStream): Value


  protected var cache: Map[Key, Value] = listValues.map(m => m.id -> m).toMap

  def keys: List[Key] = {
    cache.keys.toList
  }

  def add(id: Key, value: Value): Option[Key] = {
    Try {
      synchronized {
        writeToFile(value)
        cache += (id -> value)
      }
      id
    } toOption
  }


  /**
   * Returns the model object at location id
   *
   * @param id The key for the model
   * @return Resource if available
   */
    def get(id: Key): Option[Value] = {
      cache.get(id)
    }

  /**
   * Attempts to read all the objects out from the storage dir
   *
   * @return
   */
  protected def listValues: List[Value] = {
    listDirectories(rootDir)
      .flatMap(toKeyOption)
      .map(getPath)
      .flatMap(readFromFile)
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

  protected def toKeyOption(s: String): Option[Key] = {
    Try(s.toInt).toOption
  }

  /**
   * Returns the location of the JSON metadata file for id
   *
   * @param id The ID for the Value
   * @return
   */
  protected def getPath(id: Key): Path = {
    Paths.get(getDirectoryPath(id).toString, s"$id.json")
  }

  protected def getDirectoryPath(id: Key): Path = {
    Paths.get(rootDir, s"$id")
  }

  /**
   * Attempts to read a JSON file and convert it into a Value
   * using the JSON reader.
   *
   * @param path Location of the JSON metadata file
   * @return
   */
  protected def readFromFile(path: Path): Option[Value] = {
    Try {
      val stream = new FileInputStream(path.toFile)
      // needs to use the pre-defined extract method...
      extract(stream)
    } match {
      case Success(value) =>
        Some(value)
      case Failure(err) =>
        println(path)
        logger.error(s"Failed to read file: ${err.getMessage}")
        None
    }
  }

  /**
   * Update the model id in the storage layer
   *
   * @param id ID to give to the element
   * @param value Value object
   * @return ID of the resource created (if any)
   */
  def update(id: Key, value: Value): Option[Key] = {
    add(id, value)
  }

  /**
   * Writes the object to disk as a serialized json string
   * at a pre-defined location based on the id.
   *
   * @param value The value to write to disk
   */
  protected def writeToFile(value: Value): Unit = {

    val str = compact(Extraction.decompose(value))

    val outputPath = getPath(value.id)

    // ensure that the directories exist...
    val dir = outputPath.toFile.getParentFile

    if (!dir.exists) dir.mkdirs

    // write the object to the file system
    Files.write(
      outputPath,
      str.getBytes(StandardCharsets.UTF_8)
    )
  }

  /**
    * Deletes the resource at the key `id`. This will also
    * delete any file resource at this key
    *
    * @param id Key for the value to be removed
    * @return Key of the removed value if successful
    */
  def remove(id: Key): Option[Key] = {
    cache.get(id) match {
      case Some(ds) =>

        // delete directory - be careful
        val dir: File = getPath(id).getParent.toFile

        synchronized {
          Try(FileUtils.deleteDirectory(dir)) match {
            case Failure(err) =>
              logger.error(s"Failed to delete directory: ${err.getMessage}")
              None
            case _ =>
              cache -= id
              Some(id)
          }
        }
      case _ =>
        logger.error(s"Resource not found: $id")
        None
    }
  }

}

/**
 * Object for storing models
 */
object ModelStorage extends Storage[ModelID, Model] {

  def rootDir: String = new File(Config.ModelStorageDir).getAbsolutePath

  def extract(stream: FileInputStream): Model = {
    parse(stream).extract[Model]
  }

  def writeModel(id: ModelID, learntModel: SerializableMLibClassifier): Boolean = {
    val writePath = Paths.get(getDirectoryPath(id).toString, s"$id.rf").toString

    val out = Try(new ObjectOutputStream(new FileOutputStream(writePath)))
    print(s"Writing model rf:  $writePath")
    out match {
      case Failure(err) =>
        logger.error(s"Failed to write model: ${err.getMessage}")
        false
      case Success(f) =>
        f.writeObject(learntModel)
        f.close()
        true
    }
  }

  def identifyPaths(id: ModelID): Option[ModelTrainerPaths] = {
    val modelDir = getDirectoryPath(id).toString
    val wsDir = Paths.get(modelDir, s"workspace").toString

    println(s"modelDir: $modelDir, wsDir: $wsDir, rootDir: $rootDir")

    ModelStorage.get(id)
      .map(cm =>
        ModelTrainerPaths(curModel = cm,
          workspacePath = wsDir,
          featuresConfigPath = Paths.get(wsDir, "features_config.json").toString,
          costMatrixConfigPath = Paths.get(wsDir, s"cost_matrix_config.json").toString,
          labelsDirPath = Paths.get(wsDir, s"labels").toString))
  }
}

/**
 * Object for storing the datasets. Note that the DatasetStorage
 * has a separate functionality to store raw files in the same
 * folder as the the config json.
 */
object DatasetStorage extends Storage[DataSetID, DataSet] {

  def rootDir: String = new File(Config.DatasetStorageDir).getAbsolutePath

  def extract(stream: FileInputStream): DataSet = {
    parse(stream).extract[DataSet]
  }

  /**
   * Adds a file resource into the storage system
   *
   * @param id The id for the storage element
   * @param stream The input stream
   * @return The path to the resource (if successful)
   */
  def addFile(id: DataSetID, fs: FileStream): Option[Path] = {

    val ext = FilenameUtils.getExtension(fs.name).toLowerCase

    val outputPath = Paths.get(this.getPath(id).getParent.toString, s"$id.$ext")

    Try {
      // ensure that the directories exist...
      outputPath.toFile.getParentFile.mkdirs

      // copy the file portion over into the output path
      Files.copy(
        fs.stream,
        outputPath,
        StandardCopyOption.REPLACE_EXISTING
      )

      outputPath

    } toOption
  }

  /**
    * Return a list of paths where csv resources are stored
    */
  def getCSVResources: List[String] = {
    cache.values.map(_.path.toString).filter(_.endsWith("csv")).toList
  }

}
