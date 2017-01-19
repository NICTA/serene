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
import java.nio.file.{Files, Path, Paths}

import au.csiro.data61.core.types.Model
import org.apache.spark.ml.PipelineModel

import au.csiro.data61.core.Serene
import au.csiro.data61.core.types.ModelTypes.{ModelID, Status, TrainState}
import org.apache.commons.io.FileUtils
import org.joda.time.DateTime
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.language.postfixOps
import scala.util.{Failure, Try}


/**
 * Object for storing models
 */
object ModelStorage extends Storage[ModelID, Model] {

  implicit val keyReader: Readable[Int] = Readable.ReadableInt

  def rootDir: String = new File(Serene.config.modelStorageDir).getAbsolutePath

  def extract(stream: FileInputStream): Model = {
    parse(stream).extract[Model]
  }

  /**
    * Attempts to read all the objects out from the storage dir
    *
    * Note that here we do a basic error check and reset all the
    * paused-state 'TRAINING' models back to untrained.
    *
    * @return
    */
  override def listValues: List[Model] = {
    super.listValues
      .map {
        case model =>
          if (model.state.status == Status.BUSY) {
            val newModel = model.copy(
              state = model.state.copy(status = Status.UNTRAINED)
            )
            update(model.id, newModel)
            newModel
          } else {
            model
          }
      }
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
        val modelFile = defaultModelPath(id)

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

  def defaultModelPath(id: ModelID): Path = Paths.get(getPath(id).getParent.toString, "model")

  /**
    * Writes the MLib classifier object to a file at address `id`
    *
    * @param id    The id key for the model
    * @param model The trained model
    * @return
    */
  def addModel(id: ModelID, model: PipelineModel): Option[Path] = {

    // original file name is important to the classifier
    val outputPath = defaultModelPath(id)

    logger.info(s"Writing model to: $outputPath")

    Try {
      // ensure that the directories exist...
      outputPath.toFile.getParentFile.mkdirs

      // copy the file portion over into the output path
      model.write.overwrite().save(outputPath.toString)

      // now update the model
      synchronized {
        for {
          model <- ModelStorage.get(id)
          // we now update the model with the training information...
          newModel = model.copy(modelPath = Some(outputPath))
          id <- ModelStorage.update(id, newModel)
        } yield outputPath
      }

      outputPath

    } toOption
  }

  /**
    * updates the training state of model `id`
    *
    * Note that when we update, we need to keep the model level 'dateModified' to
    * ensure that the model parameters remains static for dataset comparisons.
    *
    * @param id     The key for the model
    * @param status The current status of the model training.
    * @return
    */
  def updateTrainState(id: ModelID,
                       status: Status,
                       msg: String = ""
                      ): Option[TrainState] = {
    synchronized {
      for {
        model <- ModelStorage.get(id)
        // state dates should not be changed if changeDate is false
        trainState = TrainState(status, msg, DateTime.now)
        // we now update the model with the training information...
        newModel = model.copy(
          state = trainState,
          dateModified = model.dateModified
        )
        id <- ModelStorage.update(id, newModel)
      } yield trainState
    }
  }

}
