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

import java.io.File
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.util.Date

import org.apache.commons.io.FileUtils

import scala.util.{Failure, Random, Try}

import DataSetTypes._

import scala.language.postfixOps


/**
 * Holder for the rest of the code. Here we assume there is
 * some sort of database that holds the datasets. We require
 * methods that add a dataset, that can update a description,
 * or can update the type map.
 */
object StorageLayer {

  val StorageDir = "/tmp/junk"

  val DefaultSampleSize = 5

  var datasets = Map.empty[DataSetID, DataSet]

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
    val outputMetaPath = Paths.get(StorageDir, s"$id", s"$id.json")

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
      columns = getColumns(id),
      filename = fileStream.name,
      path = outputPath,
      typeMap = typeMap,
      description = description,
      dateCreated = new Date(),
      dateModified = new Date()
    )

    synchronized { datasets += (id -> createdDataSet) }

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

        Try(FileUtils.deleteDirectory(dir)) match {
          case Failure(err) =>
            throw new Exception(s"Failed to delete directory: ${err.getMessage}")
          case _ =>
            synchronized { datasets -= id }
            Some(id)
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
        dateModified = new Date
      )
      synchronized { datasets += (id -> ds) }
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

      val newColumns = updateColumns(ds, typeMap)

      val newDS = datasets(id).copy(
        typeMap = typeMap,
        columns = newColumns,
        dateModified = new Date
      )

      synchronized { datasets += (id -> newDS) }
    }
  }

  /**
   * Returns updated columns on a dataset given a typeMap
   *
   * @param ds The target dataset
   * @param typeMap The typemap to apply to the columns
   * @return The new set of columns. In errors, the original column will be returned
   */
  private def updateColumns(ds: DataSet, typeMap: TypeMap): List[Column[Any]] = {

    val cols = ds.columns

    // this is more of a warning - should this throw an error?
    typeMap.keys.foreach { key =>
      if (!cols.map(_.name).contains(key)) {
        println(s"$key does not exist in columns!")
      }
    }

    // TODO: change the types of the Column[T] sample types!
    // here we assign the logical types to the column objects...
    cols.map {
      // for each column, grab the new type from the typeMap...
      col => typeMap get col.name match {
        case Some(newType) =>
          // if we find one, then we can update. If it is not
          // a real type, we can ignore
          val newLogType = LogicalType.lookup(newType)
          col.copy(logicalType = newLogType getOrElse col.logicalType)
        case _ =>
          col
      }
    }
  }

  /**
   * Return some random column objects for a dataset
   *
   * @param id The id key for the dataset
   * @return Returns a list of example columns
   */
  def getColumns(id: DataSetID, n: Int = DefaultSampleSize): List[Column[Any]] = {
    List(
      Column[String]("name",  genID, id, List.fill(n)(genAlpha),      LogicalType.STRING),
      Column[Int](   "addr",  genID, id, List.fill(n)(genID),         LogicalType.INTEGER),
      Column[Double]("phone", genID, id, List.fill(n)(genID.toFloat), LogicalType.FLOAT),
      Column[String]("junk",  genID, id, List.fill(n)(genAlpha),      LogicalType.STRING)
    )
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