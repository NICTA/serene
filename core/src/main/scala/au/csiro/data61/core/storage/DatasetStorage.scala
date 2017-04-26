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
package au.csiro.data61.core.storage

import java.io._
import java.nio.file.{Files, Path, Paths, StandardCopyOption}

import au.csiro.data61.core.Serene
import au.csiro.data61.core.api.FileStream
import au.csiro.data61.types.ColumnTypes.ColumnID
import au.csiro.data61.types.DataSetTypes._
import au.csiro.data61.types._
import org.apache.commons.io.FilenameUtils
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.language.postfixOps
import scala.util.{Failure, Success, Try}


/**
 * Object for storing the datasets. Note that the DatasetStorage
 * has a separate functionality to store raw files in the same
 * folder as the the config json.
 */
object DatasetStorage extends Storage[DataSetID, DataSet] {

  override implicit val keyReader: Readable[Int] = Readable.ReadableInt

  override def rootDir: String = new File(Serene.config.storageDirs.dataset).getAbsolutePath

  def columnMap: Map[ColumnID, Column[Any]] = cache.values
    .flatMap(_.columns)
    .map(col => col.id -> col)
    .toMap

  def columnNameMap: Map[(DataSetID, String), Column[Any]] = cache.values
    .flatMap(_.columns)
    .map(col => (col.datasetID, col.name) -> col)
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
    val dsName = FilenameUtils.getBaseName(fs.name).toLowerCase
    logger.info(s"Adding file path: name=$dsName, extension=$ext")

    // original file name is important to the classifier
    val outputPath = Paths.get(getPath(id).getParent.toString, s"$dsName.$ext")

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

    } match {
      case Success(p) =>
        logger.info(s"File added to $p.")
        Some(p)
      case Failure(err) =>
        logger.error(s"File could not be added to path $outputPath: ${err.getMessage}")
        None
    }
  }

  /**
    * Return a list of paths where csv resources are stored
    */
  def getCSVResources(datasets: List[DataSetID]): List[String] = {
    cache
      .filterKeys(datasets.contains)
      .values
      .map(_.path.toString)
      .filter(_.endsWith("csv"))
      .toList
  }

}
