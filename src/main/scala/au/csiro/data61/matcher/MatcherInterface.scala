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

import java.nio.file.Path

import DataSetTypes._
import au.csiro.data61.matcher.api.{InternalException, ParseException}
import com.github.tototoshi.csv.CSVReader
import com.typesafe.scalalogging.LazyLogging
import org.joda.time.DateTime

import scala.util.{Success, Failure, Random, Try}

import scala.language.postfixOps

/**
 * IntegrationAPI defines the interface through which requests
 * can access the underlying system. The responsibilities are
 * to parse the requests and translate into instructions for the
 * system. The return values of the functions should be simple
 * types for the web layer to translate into JSON - this includes
 * case classes as well as numbers, strings, simple maps,
 * simple arrays.
 *
 * Errors can be thrown here and they will be translated into
 * server errors or bad request errors.
 */
object MatcherInterface extends LazyLogging {

  val MissingValue = "unknown"

  val DefaultSampleSize = 15

  /**
   * Parses a servlet request to get a dataset object
   * then adds to the database, and returns the case class response
   * object.
   *
   * @param request Servlet POST request
   * @return Case class object for JSON conversion
   */
  def createDataset(request: DataSetRequest): DataSet = {

    if (request.file.isEmpty) {
      throw ParseException(s"Failed to read file request part: ${DataSetParser.FilePartName}")
    }

    val typeMap = request.typeMap getOrElse Map.empty[String, String]
    val description = request.description getOrElse MissingValue
    val id = genID

    val dataSet = for {
      fs <- request.file
      path <- StorageLayer.addFile(id, fs.stream)
      ds <- Try(DataSet(
              id = id,
              columns = getColumns(path, id, typeMap),
              filename = fs.name,
              path = path,
              typeMap = typeMap,
              description = description,
              dateCreated = DateTime.now,
              dateModified = DateTime.now
            )).toOption
      _ <- StorageLayer.addDataSet(id, ds)
    } yield ds

    dataSet getOrElse { throw InternalException(s"Failed to create resource $id") }
  }

  def datasetKeys: List[DataSetID] = {
    StorageLayer.keys
  }

  /**
   * Returns the public facing dataset from the storage layer
   *
   * @param id The dataset id
   * @return
   */
  def getDataSet(id: DataSetID): Option[DataSet] = {
    StorageLayer.getDataSet(id)
  }

  /**
   * Updates a single dataset with id key. Note that only the typemap
   * and description can be updated
   *
   * @param description Optional description for update
   * @param typeMap Optional typeMap for update
   * @param key ID corresponding to a dataset element
   * @return
   */
  def updateDataset(key: DataSetID, description: Option[String], typeMap: Option[TypeMap]): DataSet = {

    if (!StorageLayer.keys.contains(key)) {
      throw ParseException(s"Dataset $key does not exist")
    }

    val newDS = for {
      oldDS <- Try {
        StorageLayer.getDataSet(key).get
      }
      ds <- Try {
        oldDS.copy(
          description = description getOrElse oldDS.description,
          typeMap = typeMap getOrElse oldDS.typeMap,
          columns = if (typeMap.isEmpty) oldDS.columns else getColumns(oldDS.path, oldDS.id, typeMap.get),
          dateModified = DateTime.now
        )
      }
      id <- Try {
        StorageLayer.updateDataSet(key, ds).get
      } recover {
        case _ =>
          InternalException("Could not create database")
      }
    } yield ds

    newDS match {
      case Success(ds) =>
        ds
      case Failure(err) =>
        logger.error(s"Failed in UpdateDataSet for key $key")
        logger.error(err.getMessage)
        throw InternalException(err.getMessage)
    }
  }

  /**
   * Deletes the data set
   *
   * @param key Key for the dataset
   * @return
   */
  def deleteDataset(key: DataSetID): Option[DataSetID] = {
    StorageLayer.removeDataSet(key)
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
  protected def getColumns(filePath: Path,
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

    (headers zip data).zipWithIndex.map { case ((header, col), i) =>

      val logicalType = typeMap.get(header).flatMap(LogicalType.lookup)

      Column[Any](
        i,
        filePath,
        header,
        genID,
        col.size,
        dataSetID,
        genSample(retypeData(col, logicalType)).toList,
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
  protected def retypeData(data: List[String], logicalType: Option[LogicalType]): List[Any] = {
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
  protected def genID: Int = Random.nextInt(Integer.MAX_VALUE)

}