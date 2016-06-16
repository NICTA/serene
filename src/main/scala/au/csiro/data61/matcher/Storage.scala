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

import au.csiro.data61.matcher.api.FileStream
import au.csiro.data61.matcher.types.ColumnTypes.ColumnID
import au.csiro.data61.matcher.types.ModelTypes.{Model, ModelID}
import au.csiro.data61.matcher.types._
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.{FileUtils, FilenameUtils}
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.util.{Failure, Success, Try}
import DataSetTypes._

import scala.language.postfixOps
import com.nicta.dataint.matcher.serializable.SerializableMLibClassifier

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

  /**
    * Returns the location of the directory for id
    *
    * @param id The ID for the Value
    * @return
    */
  protected def getDirectoryPath(id: Key): Path = {
    Paths.get(rootDir, s"$id")
  }

  /**
    * Returns the location of the workspace directory for id
    * For now it's relevant only for models
    *
    * @param id The ID for the Value
    * @return
    */
  protected def getWSPath(id: Key): Path = {
    Paths.get(getDirectoryPath(id).toString, "workspace")
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

  /**
    * Returns the path to the serialized trained model
    *
    * @param id The `id` key to the model
    * @return Path to the binary resource
    */
  def modelPath(id: ModelID): Path = {
    Paths.get(getDirectoryPath(id).toString, s"$id.rf")
  }

  /**
    * Transforms user provided label data to the old format
    *
    * @param value The Model to write to disk
    */
  def convertLabelData(value : Model) : List[List[String]] = {
    //just fail the splitting of files if some columns from labelData are not found in colunmMap???
    List("attr_id", "class") :: // header for the file
      value.labelData           // converting to the format: "datasetID.csv/columnName,labelName"
        .map { x => {
          val col = DatasetStorage.columnMap(x._1) // lookup column in columnMap
          val ext = FilenameUtils.getExtension(col.path.toString).toLowerCase
          val dsWithExt = s"${col.datasetID}.$ext" // dataset name as it is stored in DatasetStorage
          List(s"$dsWithExt/${col.name}", x._2)
        }}
        .toList
    //    this is the way to do it if we want to check that lookups happen correctly
    //    val labelData : List[(String,String)] = value.labelData
    //      .map { x => {
    //        val column = Try(DatasetStorage.columnMap(x._1))
    //        column match{
    //          case Success(col) => (col.name, x._2)
    //          case _ => ("","")
    //        }}
    //      }
    //      .toList
    //      .filter(_ != ("",""))
  }

  /**
    * Writes the object to disk as a serialized json string
    * at a pre-defined location based on the id.
    *
    * @param value The Model to write to disk
    */
  override protected def writeToFile(value: Model): Unit = {

    super.writeToFile(value) // write model json

//  write config files according to the data integration project
    val wsDir = getWSPath(value.id).toFile  // workspace directory
    if (!wsDir.exists) wsDir.mkdirs         // create workspace directory if it doesn't exist

    //cost_matrix_config.json
    val costMatrixConfigPath = Paths.get(wsDir.toString, s"cost_matrix_config.json")
    val strCostMatrix = compact(Extraction.decompose(value.costMatrix))
    logger.info(s"Writing cost_matrix_config.json for model ${value.id}")
    Files.write(
      costMatrixConfigPath,
      strCostMatrix.getBytes(StandardCharsets.UTF_8)
    )

    //features_config.json
    val featuresConfigPath = Paths.get(wsDir.toString, "features_config.json")
    val strFeatures = compact(Extraction.decompose(value.features))
    logger.info(s"Writing features_config.json for model ${value.id}")
    Files.write(
      featuresConfigPath,
      strFeatures.getBytes(StandardCharsets.UTF_8)
    )

    //type_map.csv???
    // TODO: type-map is part of  featureExtractorParams, type-maps need to be read from datasetrepository when model gets created
    // "featureExtractorParams": [{"name": "inferred-data-type","type-map": "src/test/resources/config/type_map.csv"}]

    //labels; we want to write csv file with the following content:
    // attr_id,class
    // datasetID.csv/columnName,labelName
    val labelsDir = Paths.get(wsDir.toString, s"labels")
    if (!labelsDir.toFile.exists) labelsDir.toFile.mkdirs
    val labelsPath = Paths.get(labelsDir.toString, s"labels.csv")
    val labelData = convertLabelData(value)
    logger.info(s"Writing labels.csv for model ${value.id}")
    val out = new PrintWriter(new File(labelsPath.toString))
    labelData.foreach(line => out.println(line.mkString(",")))
    out.close()

  }

  /**
    * Updates the model at `id` and also deletes the previously
    * trained model if it exists.
    *
    * @param id ID to give to the element
    * @param value Value object
    * @return ID of the resource created (if any)
    */
  override def update(id: ModelID, value: Model): Option[ModelID] = {
    for {
      updatedID <- super.update(id, value)
      deleteOK <- deleteModel(updatedID)
    } yield deleteOK
  }

  /**
    * Deletes the model file resource if available
    *
    * @param id The key for the model object
    * @return
    */
  protected def deleteModel(id: ModelID): Option[ModelID] = {
    cache.get(id) match {
      case Some(ds) =>
        val modelFile = modelPath(id)

        if (Files.exists(modelFile)) {
          // delete model file - be careful
          synchronized {
            Try(FileUtils.deleteQuietly(modelFile.toFile)) match {
              case Failure(err) =>
                logger.error(s"Failed to delete file: ${err.getMessage}")
                None
              case _ =>
                Some(id)
            }
          }
        } else {
          Some(id)
        }
      case _ =>
        logger.error(s"Resource not found: $id")
        None
    }
  }

  /**
    * Write learnt model file for id
    */
  def writeModel(id: ModelID, learntModel: SerializableMLibClassifier): Boolean = {
    val writePath = Paths.get(getWSPath(id).toString, s"$id.rf").toString

    val out = Try(new ObjectOutputStream(new FileOutputStream(writePath)))
    logger.info(s"Writing model rf:  $writePath")
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

  /**
    * Identify paths which are needed to train the model at id
    */
  def identifyPaths(id: ModelID): Option[ModelTrainerPaths] = {
    val wsDir = getWSPath(id).toString
    logger.info(s"Identifying paths for the model $id")
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

  def columnMap: Map[ColumnID, Column[Any]] = cache.values
    .flatMap(_.columns)
    .map(col => col.id -> col)
    .toMap

  def extract(stream: FileInputStream): DataSet = {
    parse(stream).extract[DataSet]
  }

  /**
   * Adds a file resource into the storage system
   *
   * @param id The id for the storage element
   * @param fs The input stream
   * @return The path to the resource (if successful)
   */
  def addFile(id: DataSetID, fs: FileStream): Option[Path] = {

    val ext = FilenameUtils.getExtension(fs.name).toLowerCase // original file extension

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
