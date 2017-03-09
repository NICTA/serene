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

import java.io.{File, FileInputStream}
import java.nio.file.{Files, Path, Paths}

import au.csiro.data61.core.Serene
import au.csiro.data61.core.api.NotFoundException
import au.csiro.data61.types.Training.{Status, TrainState, _}
import au.csiro.data61.types.SsdTypes._
import org.apache.commons.io.FileUtils
import org.joda.time.DateTime
import org.json4s.jackson.JsonMethods._

import scala.util.{Failure, Try}

/**
  * AlignmentStorage holds the Alignment objects in a key-value store
  */
object OctopusStorage extends Storage[OctopusID, Octopus] {

  val DefaultAlignmentDir = "alignment-graph"
  val GraphJson = "graph.json"

  implicit val keyReader: Readable[Int] = Readable.ReadableInt

  def rootDir: String = new File(Serene.config.storageDirs.octopus).getAbsolutePath

  def getAlignmentDirPath(id: OctopusID): Path = {
    Paths.get(getDirectoryPath(id).toString, DefaultAlignmentDir)
  }

  def getAlignmentGraphPath(id: OctopusID): Path = {
    Paths.get(getDirectoryPath(id).toString, DefaultAlignmentDir, GraphJson)
  }

  def extract(stream: FileInputStream): Octopus = {
    parse(stream).extract[Octopus]
  }

  /**
    * Updates the training state of octopus `id`.
    * Note that when we update, we need to keep the octopus level 'dateModified' to
    * ensure that the octopus parameters remains static for ssd/dataset comparisons.
    *
    * @param id     The key for the octopus
    * @param status The current status of the octopus training.
    * @return
    */
  def updateTrainState(id: OctopusID,
                       status: Status,
                       msg: String = "",
                       path: Option[Path] = None
                      ): Option[TrainState] = {
    synchronized {
      for {
        model <- OctopusStorage.get(id)
        // state dates should not be changed if changeDate is false
        trainState = TrainState(status, msg, DateTime.now)
        // we now update the model with the training information...
        newModel = model.copy(
          state = trainState,
          dateModified = model.dateModified
        )
        id <- OctopusStorage.update(id, newModel)
      } yield trainState
    }
  }

  /**
    * We need to delete the alignment graph explicitly in case of changes to the Octopus.
    * Also, in case training for octopus fails, this alignment directory should also be deleted.
    * The issue is that Karma will not work properly if we leave the previous version of the alignment graph.
    * Karma allows incremental learning of the alignment graph.
    *
    * @param id octopus id
    */
  def deleteAlignmetDir(id: OctopusID): Option[OctopusID] = {
    logger.debug(s"Deleting alignment directory for octopus $id")
    // delete directory - be careful
    val dir: File = getAlignmentDirPath(id).toFile

    synchronized {
      Try(FileUtils.deleteDirectory(dir)) match {
        case Failure(err) =>
          logger.error(s"Failed to delete alignment directory for octopus $id: ${err.getMessage}")
          None
        case _ =>
          Some(id)
      }
    }
  }

}
