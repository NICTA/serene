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
package au.csiro.data61.core.drivers

import java.io.FileReader
import java.nio.file.Path

import au.csiro.data61.core.api.{DataSetRequest, InternalException, ParseException}
import au.csiro.data61.core.storage._
import au.csiro.data61.types.ColumnTypes._
import au.csiro.data61.types.{Column, DataSet, LogicalType}
import au.csiro.data61.types.DataSetTypes._
import au.csiro.data61.types.ModelTypes.ModelID
import au.csiro.data61.types.SsdTypes.{OctopusID, SsdID}
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.csv.CSVFormat
import org.joda.time.DateTime
import scala.collection.JavaConverters._
import language.implicitConversions
import scala.util.{Try, Success, Failure}

object DataSetInterface extends StorageInterface[DatasetKey, DataSet] with LazyLogging {

  protected val DefaultSampleSize = 15

  protected val DefaultSeed = 1234

  override protected val storage = DatasetStorage

  protected def missingReferences(resource: DataSet): StorageDependencyMap = {
    // Dataset does not have any references to be checked
    StorageDependencyMap()
  }

  protected def dependents(resource: DataSet): StorageDependencyMap = {
    // column ids from this dataset
    val colIds: Set[ColumnID] = get(resource.id)
      .map(_.columns.map(_.id))
      .getOrElse(List.empty[ColumnID])
      .toSet

    // SSDs which have columns from this dataset
    val ssdRefIds: List[SsdID] = SsdStorage.keys
      .flatMap(SsdStorage.get)
      .map(x => (x.id, x.attributes.map(_.id))) // (SsdID, List[ColumnID])
      .map {
      case (ssdID, cols) => (ssdID, cols.filter(colIds.contains)) // filter out columns which are in this dataset
    }
      .filter(_._2.nonEmpty) // filter those SsdID which contain columns from this dataset
      .map(_._1)

    // models which refer to this dataset
    val modelRefIds: List[ModelID] = ModelStorage.keys
      .flatMap(ModelStorage.get)
      .map(x => (x.id, x.refDataSets.toSet))
      .filter(_._2.contains(resource.id))
      .map(_._1)

    // octopi which refer to modelRefIds or ssdRefIds
    val octoRefIds: List[OctopusID] = OctopusStorage.keys
      .flatMap(OctopusStorage.get)
      .map(x => (x.id, x.lobsterID, x.ssds.toSet))
      .filter {
        case (octoID, modelID, ssds) =>
          modelRefIds.toSet.contains(modelID) || (ssdRefIds.toSet & ssds).nonEmpty
      }
      .map(_._1)

    StorageDependencyMap(model = modelRefIds, ssd = ssdRefIds, octopus = octoRefIds)
  }

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
      logger.error(s"Failed to read file request part")
      throw ParseException(s"Failed to read file request part")
    }

    val typeMap = request.typeMap getOrElse Map.empty[String, String]
    val description = request.description getOrElse MissingValue
    val id = Generic.genID
    logger.info(s"Writing dataset $id")

    val now = DateTime.now
    val dataSet = for {
      fs <- request.file
      path <- DatasetStorage.addFile(id, fs)
      ds <- Try(DataSet(
        id = id,
        columns = getColumns(path, id, typeMap),
        filename = fs.name,
        path = path,
        typeMap = typeMap,
        description = description,
        dateCreated = now,
        dateModified = now
      )).toOption
      _ <- add(ds)
    } yield ds

    dataSet getOrElse {
        throw InternalException(s"Failed to create resource $id")
    }
//    dataSet match {
//      case Success(ds) =>
//        ds
//      case Failure(err) =>
//        logger.error(s"Failed to create resource $id due to ${err.getMessage}")
//        throw InternalException(s"Failed to create resource $id")
//    }
  }

  /**
    * Returns the public facing dataset from the storage layer
    *
    * @param id The dataset id
    * @return
    */
  def getDataSet(id: DataSetID, colSize: Option[Int]): Option[DataSet] = {
    if (colSize.isEmpty) {
      DatasetStorage.get(id)
    } else {
      DatasetStorage.get(id).map(ds => {
        val sampleColumns = getColumns(ds.path, ds.id, ds.typeMap, colSize.get)

        ds.copy(
          columns = ds.columns.zip(sampleColumns).map {
            case (column, sampleColumn) => column.copy(sample = sampleColumn.sample)
          }
        )
      })
    }
  }

  /**
    * Updates a single dataset with id key. Note that only the typemap
    * and description can be updated
    *
    * @param description Optional description for update
    * @param typeMap     Optional typeMap for update
    * @param key         ID corresponding to a dataset element
    * @return
    */
  def updateDataset(key: DataSetID, description: Option[String], typeMap: Option[TypeMap]): DataSet = {

    if (!DatasetStorage.keys.contains(key)) {
      throw ParseException(s"Dataset $key does not exist")
    }

    val newDS = for {
      oldDS <- get(key)

      ds = oldDS.copy(
        description = description getOrElse oldDS.description,
        typeMap = typeMap getOrElse oldDS.typeMap,
        columns = if (typeMap.isEmpty) oldDS.columns else getColumns(oldDS.path, oldDS.id, typeMap.get),
        dateModified = DateTime.now
      )

      _ <- update(ds)

    } yield ds

    newDS match {
      case Some(ds) =>
        ds
      case _ =>
        logger.error(s"Failed in UpdateDataSet for key $key")
        throw InternalException(s"Failed in UpdateDataSet for key $key")
    }
  }


  /**
    * Transpose list which is not necessarily of the same size
    * @param xs list of objects of type A
    * @return
    */
  protected def csvTranspose(xs: List[List[String]]): List[List[String]] = xs.filter(_.nonEmpty) match {
    case Nil    =>  Nil
    case ys: List[List[String]] => ys.map{ _.head }::csvTranspose(ys.map{ _.tail })
  }

  /**
    * Helper function to read the csv file.
    *
    * @param filePath
    * @param sampleCount
    * @return
    */
  protected def readColumns(filePath: Path, sampleCount: Int = DefaultSampleSize): List[List[String]] = {

    // TODO: Update this to transpose to an Iterator[Iterator[String]] to prevent pulling whole file into memory

    val sampleBound = 4 * sampleCount

    // first load a CSV object...
    val csv = CSVFormat.RFC4180.parse(new FileReader(filePath.toFile))

    logger.debug("Pulling columns into a row list...")
    // pull into a row List of List[String]
    csv
      .iterator
      .asScala
      .take(sampleBound)
      .map { row => (0 until row.size()).map(row.get).toList }
      .filter { line => !line.forall(_.length == 0)} // we filter out rows which contain empty vals
      .toList
      .transpose

//    csvTranspose(csv1)
  }

  /**
    * Return some random column objects for a dataset
    *
    * @param filePath    Full path to the file
    * @param dataSetID   ID of the parent dataset
    * @param n           Number of samples in the sample set
    * @param headerLines Number of header lines in the file
    * @param seed        The random seed for the shuffle
    * @return A list of Column objects
    */
  protected def getColumns(filePath: Path,
                           dataSetID: DataSetID,
                           typeMap: TypeMap,
                           n: Int = DefaultSampleSize,
                           headerLines: Int = 1,
                           seed: Int = DefaultSeed): List[Column[Any]] = {
    logger.debug("Getting random sample of columns from dataset...")
    // note that we only take a sample from the first 4n samples. Otherwise
    // we need to pull the whole file into memory to get say 10 samples...
    val columns = Try { readColumns(filePath)} match {
      case Success(cols) =>
        logger.debug("...columns read from file...")
        cols
      case Failure(err) =>
        logger.error(s"Failed to read columns for dataset $dataSetID: ${err.getMessage}")
        throw InternalException(s"Failed to read columns for dataset $dataSetID")
    }

    val headers = columns.map(_.take(headerLines).mkString("_"))
    val data = columns.map(_.drop(headerLines))

    // generate random samples...
    val rnd = new scala.util.Random(seed=seed)

    // we create a set of random indices that will be consistent across the
    // columns in the dataset.
    val indices = if (data.isEmpty) {
      Array.empty[Int]
    } else {
      Array.fill(n)(rnd.nextInt(data.head.size - 1))
    }

    logger.debug(s"... creating set of column objects: #headers=${headers.size}...")
    // now we recombine with the headers and an index to create the
    // set of column objects...
    (headers zip data).zipWithIndex.map { case ((header, col), i) =>
      val logicalType = typeMap.get(header).flatMap(LogicalType.lookup)
      val typedData = retypeData(col, logicalType)
      Column[Any](
        i,
        filePath,
        header,
        Generic.genID,
        col.size,
        dataSetID,
        indices.map(typedData(_)).toList,
        logicalType getOrElse LogicalType.STRING)
    }
  }

  /**
    * Changes the type of the csv data, very crude at the moment
    *
    * @param data        The original csv data
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

}
