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

import au.csiro.data61.matcher.storage.{DatasetStorage, ModelStorage}
import types.ColumnTypes.ColumnID
import types.ModelTypes.{TrainState, Status, ModelID, Model}
import types._
import DataSetTypes._
import api.{DataSetRequest, ModelRequest, InternalException, ParseException}

import java.nio.file.Path
import com.github.tototoshi.csv.CSVReader
import com.typesafe.scalalogging.LazyLogging
import org.joda.time.DateTime
import scala.util.{Success, Failure, Random, Try}
import scala.language.postfixOps

import scala.concurrent.{ExecutionContext, Future}

import scala.concurrent.ExecutionContext.Implicits.global

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
    * Parses a model request to construct a model object
    * then adds to the database, and returns the case class response
    * object.
    *
    * @param request POST request with model information
    * @return Case class object for JSON conversion
    */
  def createModel(request: ModelRequest): Model = {

    val id = genID

    // build the model from the request, adding defaults where necessary
    val modelOpt = for {
        colMap <- Some(DatasetStorage.columnMap)
        userData <- Some(
          request.labelData.getOrElse(Map.empty[ColumnID, String])
        )
        (keysIn, keysOut) <- Option {
          userData.keySet.partition(colMap.keySet.contains)
        }// keysIn contain those keys from userData which should be kept and written to the model file
        // TODO: check that provided mappings for columns in userData are found among labels
        model <- Try {
          Model(
            id = id,
            description = request.description.getOrElse(MissingValue),
            modelType = request.modelType.getOrElse(ModelType.RANDOM_FOREST),
            classes = request.labels.getOrElse(List()),
            features = request.features.getOrElse(FeaturesConfig(Set.empty[String], Set.empty[String], Map.empty[String, Map[String, String]])),
            costMatrix = request.costMatrix.getOrElse(List()),
            resamplingStrategy = request.resamplingStrategy.getOrElse(SamplingStrategy.RESAMPLE_TO_MEAN),
            labelData = userData.filterKeys(keysIn),
            refDataSets = colMap.filterKeys(keysIn).values.map(_.datasetID).toList,
            state = TrainState(Status.UNTRAINED, "", DateTime.now, DateTime.now),
            dateCreated = DateTime.now,
            dateModified = DateTime.now)
        } toOption

        _ <- ModelStorage.add(id, model)

      } yield {
      if (keysOut.nonEmpty) {
        logger.warn(s"Following column keys do not exist: ${keysOut.mkString(",")}")
      }
      model
    }
    modelOpt getOrElse { throw InternalException("Failed to create resource.") }
  }

  /**
   * Returns the public facing model from the storage layer
   *
   * @param id The model id
   * @return
   */
  def getModel(id: ModelID): Option[Model] = {
    ModelStorage.get(id)
  }

  /**
   * Trains the model
   *
   * @param id The model id
   * @return
   */
  def trainModel(id: ModelID): Option[TrainState] = {

    // crude concurrency
    val state = ModelStorage.get(id).map(_.state)
    val status = state.map(_.status)

    status.flatMap {

      case Status.BUSY =>
        // if it is complete or pending, just return the value
        logger.info("Returning cached state")
        state

      case Status.COMPLETE | Status.UNTRAINED | Status.ERROR => {
        // in the background we launch the training...
        logger.info("Launching training.....")
        launchTraining(id)
        // first we set the model state to training....
        ModelStorage.updateTrainState(id, Status.BUSY)
      }
    }
  }

  /**
   * Asynchronously launch the training process, and write
   * to storage once complete. The actual state will be
   * returned from the above case when re-read from the
   * storage layer.
   *
   * @param id Model key for the model to be launched
   */
  private def launchTraining(id: ModelID)(implicit ec: ExecutionContext): Unit = {

    Future {
      // proceed with training...
      ModelTrainer.train(id).map {
        ModelStorage.writeModel(id, _)
      }
    } onComplete {
      case Success(Some(true)) =>
        ModelStorage.updateTrainState(id, Status.COMPLETE)
      case Success(Some(false)) =>
        logger.error(s"Failed to write trained model")
        ModelStorage.updateTrainState(id, Status.ERROR, s"Failed to write trained model")
      case Success(None) =>
        logger.error(s"Failed to identify model paths")
        ModelStorage.updateTrainState(id, Status.ERROR, s"Failed to identify model paths")
      case Failure(err) =>
        val msg = s"Failed to train model: ${err.getMessage}"
        logger.error(msg)
        ModelStorage.updateTrainState(id, Status.ERROR, msg)
    }
  }

  /**
   * Parses a model request to construct a model object
   * for updating. The index is searched for in the database,
   * and if update is successful, returns the case class response
   * object.
   *
   * @param request POST request with model information
   * @return Case class object for JSON conversion
   */
  def updateModel(id: ModelID, request: ModelRequest): Model = {

    // build the model from the request, adding defaults where necessary
    val modelOpt = for {
      m <- ModelStorage.get(id)
      model <- Try {
        m.copy(
          description = request.description.getOrElse(m.description),
          modelType = request.modelType.getOrElse(m.modelType),
          classes = request.labels.getOrElse(m.classes),
          features = request.features.getOrElse(FeaturesConfig(Set.empty[String], Set.empty[String], Map.empty[String, Map[String, String]])),
          costMatrix = request.costMatrix.getOrElse(m.costMatrix),
          resamplingStrategy = request.resamplingStrategy.getOrElse(m.resamplingStrategy),
          labelData = request.labelData.getOrElse(m.labelData),
          dateModified = DateTime.now)
      }.toOption
      _ <- ModelStorage.update(id, model)
    } yield model

    modelOpt getOrElse { throw InternalException("Failed to update resource.") }
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
      throw ParseException(s"Failed to read file request part")
    }

    val typeMap = request.typeMap getOrElse Map.empty[String, String]
    val description = request.description getOrElse MissingValue
    val id = genID

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
              dateCreated = DateTime.now,
              dateModified = DateTime.now
            )).toOption
      _ <- DatasetStorage.add(id, ds)
    } yield ds

    dataSet getOrElse { throw InternalException(s"Failed to create resource $id") }
  }

  def datasetKeys: List[DataSetID] = {
    DatasetStorage.keys
  }

  def modelKeys: List[ModelID] = {
    ModelStorage.keys
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
      DatasetStorage.get(id).map(ds =>
        ds.copy(columns = getColumns(ds.path, ds.id, ds.typeMap, colSize.get))
      )
    }
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

    if (!DatasetStorage.keys.contains(key)) {
      throw ParseException(s"Dataset $key does not exist")
    }

    val newDS = for {
      oldDS <- Try {
        DatasetStorage.get(key).get
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
        DatasetStorage.update(key, ds).get
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
    DatasetStorage.remove(key)
  }

  /**
   * Deletes the model
   *
   * @param key Key for the model
   * @return
   */
  def deleteModel(key: ModelID): Option[ModelID] = {
    ModelStorage.remove(key)
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
    // TODO: Get this out of memory!
    val csv = CSVReader.open(filePath.toFile)
    val columns = csv.all.transpose
    val headers = columns.map(_.take(headerLines).mkString("_"))
    val data = columns.map(_.drop(headerLines))
    val size = columns.headOption.map(_.size).getOrElse(0)

    // generate random samples...
    val rnd = new scala.util.Random(0)

    // we create a set of random indices that will be consistent across the
    // columns in the dataset.
    val indices = Array.fill(n)(rnd.nextInt(size))

    // now we recombine with the headers and an index to create the
    // set of column objects...
    (headers zip data).zipWithIndex.map { case ((header, col), i) =>

      val logicalType = typeMap.get(header).flatMap(LogicalType.lookup)
      val typedData = retypeData(col, logicalType)

      Column[Any](
        i,
        filePath,
        header,
        genID,
        col.size,
        dataSetID,
        indices.map(typedData(_)).toList,
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
