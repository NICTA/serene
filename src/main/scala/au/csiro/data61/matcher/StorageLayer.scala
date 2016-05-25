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

import java.io.{FileInputStream, File}
import java.nio.charset.StandardCharsets
import java.nio.file.{Path, Files, Paths, StandardCopyOption}

import com.github.tototoshi.csv.CSVReader
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FileUtils
import org.joda.time.DateTime

import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.util.{Success, Failure, Random, Try}

import DataSetTypes._

import scala.language.postfixOps


/**
 * Holder for the rest of the code. Here we assume there is
 * some sort of database that holds the datasets. We require
 * methods that add a dataset, that can update a description,
 * or can update the type map.
 */
object StorageLayer extends LazyLogging with MatcherJsonFormats {

  val StorageDir = "/tmp/junk"

  val DefaultSampleSize = 15

  var datasets = findDataSets.map(ds => ds.id -> ds).toMap

  def toIntOption(s: String): Option[Int] = {
    Try(s.toInt).toOption
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
        logger.error(s"Failed to open dir $StorageDir")
        List.empty[String]
    }
  }

  /**
   * Attempts to read all the datasets out from the storage dir
   *
   * @return
   */
  protected def findDataSets: List[DataSet] = {
    listDirectories(StorageDir)
      .flatMap(toIntOption)
      .map(getMetaPath)
      .flatMap(readDataSetFromFile)
  }

  /**
   * Returns the location of the JSON metadata file for DataSet id
   *
   * @param id The ID for the DataSet
   * @return
   */
  protected def getMetaPath(id: DataSetID): Path = {
    Paths.get(StorageDir, s"$id", s"$id.json")
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
   * Writes the dataset ds to disk as a serialized json string
   * at a pre-defined location based on the id.
   *
   * @param ds The dataset to write to disk
   */
  protected def writeDataSetToFile(ds: DataSet): Unit = {

    val str = compact(Extraction.decompose(ds))

    // write the dataset to the file system
    Files.write(
      getMetaPath(ds.id),
      str.getBytes(StandardCharsets.UTF_8)
    )
  }

  /**
   * Adds a dataset to the object state. This requires types from
   * the parsed request object. This will return a new dataset object.
   *
   * @param fileStream The FileStream object pointing the request data
   * @param description The description provided by the user
   * @param typeMap The typeMap for user specified types e.g. int, string, bool, factor, float
   * @return
   */
  def addDataset(fileStream: FileStream,
                 description: String,
                 typeMap: TypeMap): DataSet = {

    val id = genID

    val outputPath = Paths.get(StorageDir, s"$id", s"$id.txt")

    // ensure that the directories exist...
    outputPath.toFile.getParentFile.mkdirs

    // copy the file portion over into the output path
    Files.copy(
      fileStream.stream,
      outputPath,
      StandardCopyOption.REPLACE_EXISTING
    )

    val createdDataSet = DataSet(
      id = id,
      columns = getColumns(outputPath, id, typeMap),
      filename = fileStream.name,
      path = outputPath,
      typeMap = typeMap,
      description = description,
      dateCreated = DateTime.now,
      dateModified = DateTime.now
    )

    synchronized {
      writeDataSetToFile(createdDataSet)
      datasets += (id -> createdDataSet)
    }

    createdDataSet
  }

  /**
   * Deletes the dataset and the associated resource at `id`
   *
   * @param id The dataset id key
   * @return
   */
  def deleteDataset(id: DataSetID): Option[DataSetID] = {
    datasets.get(id) match {
      case Some(ds) =>

        // TODO: Abstract this DB layer away...
        // delete directory - be careful
        val dir: File = ds.path.getParent.toFile

        synchronized {
          Try(FileUtils.deleteDirectory(dir)) match {
            case Failure(err) =>
              throw new Exception(s"Failed to delete directory: ${err.getMessage}")
            case _ =>
              datasets -= id
              Some(id)
          }
        }
      case _ =>
        None
    }
  }

  /**
   * Updates the description. Note that this is a method an will update
   * the mutable data set map state.
   *
   * @param id The id for the data set
   * @param description The user supplied meta data description
   */
  def updateDescription(id: DataSetID, description: String): Unit = {

    if (datasets.contains(id)) {

      val ds = datasets(id).copy(
        description = description,
        dateModified = DateTime.now
      )
      synchronized {
        writeDataSetToFile(ds)
        datasets += (id -> ds)
      }
    }
  }

  /**
   * Updates the dataset at id given a new type map. Note that this
   * is a method and updates the data set map state.
   *
   * @param id The id for the data set
   * @param typeMap The user specified typeMap with column names -> type string e.g. string, float, int, factor, bool
   */
  def updateTypeMap(id: DataSetID, typeMap: TypeMap): Unit = {

    if (datasets.contains(id)) {

      val ds = datasets(id)

      val newDS = ds.copy(
        typeMap = typeMap,
        columns = getColumns(ds.path, id, typeMap), //updateColumns(ds, typeMap),
        dateModified = DateTime.now
      )

      synchronized {
        writeDataSetToFile(newDS)
        datasets += (id -> newDS)
      }
    }
  }

  /**
   * Return some random column objects for a dataset
   *
   * @param filePath Full path to the file
   * @param dataSetID ID of the parent dataset
   * @param n Number of samples in the sample set
   * @param headerLines Number of header lines in the file
   * @return A list of Column objects
   */
  def getColumns(filePath: Path,
                 dataSetID: DataSetID,
                 typeMap: TypeMap,
                 n: Int = DefaultSampleSize,
                 headerLines: Int = 1): List[Column[Any]] = {

    // generate random samples...
    val rnd = new scala.util.Random(0)
    def genSample(col: List[Any]) = Array.fill(n)(col(rnd.nextInt(col.size)))

    // TODO: Get this out of memory!
    val csv = CSVReader.open(filePath.toFile)
    val columns = csv.all.transpose
    val headers = columns.map(_.take(headerLines).mkString("_"))
    val data = columns.map(_.drop(headerLines))

    (headers zip data).zipWithIndex.map { case ((header, x), i) =>

      val logicalType = typeMap.get(header).flatMap(LogicalType.lookup)

      Column[Any](
        i,
        filePath,
        header,
        genID,
        x.size,
        dataSetID,
        genSample(retypeData(x, logicalType)).toList,
        logicalType getOrElse LogicalType.STRING)
    }
  }

  /**
   * Changes the type of the csv data, very crude at the moment
   *
   * @param data The original csv data
   * @param logicalType The optional logical type. It will be cast to string if none.
   * @return
   */
  def retypeData(data: List[String], logicalType: Option[LogicalType]): List[Any] = {
    logicalType match {

      case Some(LogicalType.BOOLEAN) =>
        data.map(_.toBoolean)

      case Some(LogicalType.FLOAT) =>
        data.map(s => Try(s.toDouble).toOption getOrElse Double.NaN)

      case Some(LogicalType.INTEGER) =>
        data.map(s => Try(s.toInt).toOption getOrElse Int.MinValue)

      case Some(LogicalType.STRING) =>
        data

      case _ =>
        data
    }
  }

  /**
   * Generate a random positive integer id
   *
   * @return Returns a random positive integer
   */
  def genID: Int = Random.nextInt(Integer.MAX_VALUE)

  /**
   * Generate a random alphanumeric data
   *
   * @return Returns a random alphanumeric string
   */
  def genAlpha: String = Random.alphanumeric take 5 mkString

}