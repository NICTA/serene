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

import java.nio.file.{Files, Path}

import au.csiro.data61.core.api._
import au.csiro.data61.core.drivers.MatcherInterface.DataRef
import au.csiro.data61.core.storage.ModelStorage._
import au.csiro.data61.core.storage.{DatasetStorage, ModelStorage, OctopusStorage, OwlStorage}
import au.csiro.data61.types.ColumnTypes._
import au.csiro.data61.types.DataSetTypes._
import au.csiro.data61.types._
import au.csiro.data61.types.ModelTypes.{Model, ModelID}
import au.csiro.data61.types.SsdTypes._
import au.csiro.data61.types.Training.{Status, TrainState}
import com.typesafe.scalalogging.LazyLogging
import org.joda.time.DateTime

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object ModelInterface extends TrainableInterface[ModelKey, Model] with LazyLogging {

  override val storage = ModelStorage

  def checkTraining(id: Key): Boolean = {
    logger.info(s"Checking consistency of model $id")

    // make sure the datasets in the model are older
    // than the training state
    val isOK = for {
      model <- get(id)
      path = model.modelPath
      trainDate = model.state.dateChanged
      refIDs = model.refDataSets
      refs = refIDs.flatMap(DatasetStorage.get).map(_.dateModified)

      // make sure the model is complete
      isComplete = model.state.status == Status.COMPLETE

      // make sure the datasets are older than the training date
      allBefore = refs.forall(_.isBefore(trainDate))

      // make sure the model file is there...
      modelExists = path.exists(Files.exists(_))

    } yield allBefore && modelExists && isComplete

    isOK getOrElse false
  }

  protected def missingReferences(resource: Model): StorageDependencyMap = {
    logger.debug(s"Checking missing references for model ${resource.id}")

    val presentColIds: List[ColumnID] = resource.labelData.keys.toList

    // missing columns
    val colIDs: List[ColumnID] = presentColIds.filterNot(DatasetStorage.columnMap.keys.toSet.contains)

    StorageDependencyMap(column = colIDs)
  }

  protected def dependents(resource: Model): StorageDependencyMap = {
    // only octopi
    val octoRefIds: List[OctopusID] = OctopusStorage.keys
      .flatMap(OctopusStorage.get)
      .map(x => (x.id, x.lobsterID))
      .filter(_._2 == resource.id)
      .map(_._1)

    StorageDependencyMap(octopus = octoRefIds)
  }


   /**
    * createModel builds a new Model object from a ModelRequest
    *
    * @param request The request object from the API
    * @return
    */
  def createModel(request: ModelRequest): Model = {

    val id = Generic.genID

    val labelMap = request.labelData.getOrElse(Map.empty[ColumnID, String])
    val refDatasets: List[DataSetID] = DatasetStorage.columnMap
      .filterKeys(labelMap.keySet)
      .values
      .map(_.datasetID)
      .toList

    // build the model from the request, adding defaults where necessary
    val model = Model(
      id = id,
      description = request.description.getOrElse(MissingValue),
      modelType = request.modelType.getOrElse(ModelType.RANDOM_FOREST),
      classes = request.classes.getOrElse(List()),
      features = request.features.getOrElse(FeaturesConfig(Set.empty[String], Set.empty[String], Map.empty[String, Map[String, String]])),
      costMatrix = request.costMatrix.getOrElse(List()),
      resamplingStrategy = request.resamplingStrategy.getOrElse(SamplingStrategy.RESAMPLE_TO_MEAN),
      labelData = labelMap,
      refDataSets = refDatasets,
      modelPath = None,
      state = Training.TrainState(Training.Status.UNTRAINED, "", DateTime.now),
      dateCreated = DateTime.now,
      dateModified = DateTime.now,
      bagSize = request.bagSize.getOrElse(ModelTypes.defaultBagSize),
      numBags = request.numBags.getOrElse(ModelTypes.defaultNumBags)
    )

    add(model) match {
      case Some(key: ModelID) => model
      case _ =>
        logger.error(s"Failed to create model")
        throw InternalException(s"Failed to create model")
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

    logger.debug(s"Updating model $id")
    val labelsUpdated = request.labelData.isDefined
    val labelMap = request.labelData.getOrElse(Map.empty[ColumnID, String])
    val refDatasets: List[DataSetID] = DatasetStorage.columnMap
      .filterKeys(labelMap.keySet)
      .values
      .map(_.datasetID)
      .toList

    val modelOpt = for {
      old <- get(id)

      // build up the model request, and use existing if field is not present...
      updatedModel = Model(
        id = id,
        description = request.description.getOrElse(old.description),
        modelType = request.modelType.getOrElse(old.modelType),
        classes = request.classes.getOrElse(old.classes),
        features = request.features.getOrElse(old.features),
        costMatrix = request.costMatrix.getOrElse(old.costMatrix),
        resamplingStrategy = request.resamplingStrategy.getOrElse(old.resamplingStrategy),
        labelData = if (labelsUpdated) labelMap else old.labelData,
        refDataSets = if (labelsUpdated) refDatasets else old.refDataSets,
        state = Training.TrainState(Training.Status.UNTRAINED, "", DateTime.now),
        modelPath = None,
        dateCreated = old.dateCreated,
        dateModified = DateTime.now,
        bagSize = request.bagSize.getOrElse(ModelTypes.defaultBagSize),
        numBags = request.numBags.getOrElse(ModelTypes.defaultNumBags)
      )
      _ <- update(updatedModel)
    } yield updatedModel

    modelOpt getOrElse { throw InternalException("Failed to update resource.") }
  }

  /**
    * Trains the model
    *
    * @param id The model id
    * @return
    */
  def trainModel(id: ModelID, force: Boolean = false): Option[TrainState] = {

    for {
      model <- ModelStorage.get(id)
      state = model.state
      newState = state.status match {
        case Status.COMPLETE if ModelStorage.isConsistent(id) && !force =>
          logger.info(s"Model $id is already trained.")
          state
        case Status.BUSY =>
          // if it is complete or pending, just return the value
          logger.info(s"Model $id is busy.")
          state
        case Status.COMPLETE | Status.UNTRAINED | Status.ERROR =>
          // in the background we launch the training...
          logger.info("Launching training.....")
          // first we set the model state to training....
          val newState = ModelStorage.updateTrainState(id, Status.BUSY)
          launchTraining(id)
          newState.get
        case _ =>
          state
      }
    } yield newState

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
        ModelStorage.addModel(id, _)
      }
    } onComplete {
      case Success(Some(path)) =>
        // we update the status, the state date and do not delete the model.rf file
        ModelStorage.updateTrainState(id, Training.Status.COMPLETE, "", path)
      case Success(None) =>
        // we update the status, the state date and delete the model.rf file
        logger.error(s"Failed to identify model paths for $id.")
        ModelStorage.updateTrainState(id, Training.Status.ERROR, s"Failed to identify model paths.", None)
      case Failure(err) =>
        // we update the status, the state date and delete the model.rf file
        val msg = s"Failed to train model $id: ${err.getMessage}."
        logger.error(msg)
        ModelStorage.updateTrainState(id, Training.Status.ERROR, msg, None)
    }
  }

  def lobsterTraining(id: ModelID, force: Boolean = false)(implicit ec: ExecutionContext): Future[Option[Path]] = {
    Future {
      val model = ModelStorage.get(id).get
      val state = model.state
      state.status match {
        case Status.COMPLETE if ModelStorage.isConsistent(id) && !force =>
          logger.info(s"Lobster $id is already trained.")
          model.modelPath
        case Status.COMPLETE | Status.UNTRAINED | Status.ERROR =>
          logger.info("Launching lobster training.....")
          // first we set the model state to training....
          ModelStorage.updateTrainState(id, Status.BUSY)
          // in the background we launch the training...
          ModelTrainer.train(id).flatMap { ModelStorage.addModel(id, _) }
        case _ =>
          // we shouldn't actually have this option now
          model.modelPath
      }
    }
  }

  /**
    * Perform prediction using the model
    *
    * @param id The model id
    * @param datasetID Optional id of the dataset
    * @return
    */
  def predictModel(id: ModelID, datasetID : DataSetID): DataSetPrediction = {

    if (checkTraining(id)) {
      // do prediction
      logger.info(s"Launching prediction for model $id...")
      ModelPredictor.predict(id, datasetID)
    } else {
      val msg = s"Prediction failed. Model $id is not consistent. Try training the model."
      // prediction is impossible since the model has not been trained properly
      logger.warn(msg)
      throw BadRequestException(msg)
    }
  }

}
