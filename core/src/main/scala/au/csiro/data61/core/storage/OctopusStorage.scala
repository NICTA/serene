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
import au.csiro.data61.types.ModelTypes.Status.COMPLETE
import au.csiro.data61.types.ModelTypes.{Status, TrainState, _}
import au.csiro.data61.types.SSDTypes.{Octopus, OctopusID}
import org.joda.time.DateTime
import org.json4s.jackson.JsonMethods._

import scala.util.{Failure, Success}

/**
  * AlignmentStorage holds the Alignment objects in a key-value store
  */
object OctopusStorage extends Storage[OctopusID, Octopus] {

  val defaultAlignmentDir = "alignment-graph"

  override implicit val keyReader: Readable[Int] = Readable.ReadableInt

  override def rootDir: String = new File(Serene.config.datasetStorageDir).getAbsolutePath

  def getAlignmentDirPath(id: OctopusID): Path = {
    Paths.get(getDirectoryPath(id).toString, defaultAlignmentDir)
  }

  def getAlignmentGraphPath(id: OctopusID): Path = {
    Paths.get(getDirectoryPath(id).toString, defaultAlignmentDir, "graph.json")
  }

  def extract(stream: FileInputStream): Octopus = {
    parse(stream).extract[Octopus]
  }

  /**
    * updates the training state of octopus `id`
    *
    * Note that when we update, we need to keep the octopus level 'dateModified' to
    * ensure that the model parameters remains static for SSD comparisons.
    *
    * @param id     The key for the octopus
    * @param status The current status of the model training.
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
          dateModified = model.dateModified,
          alignmentDir = path//,
          //predictionPath = None
        )
        id <- OctopusStorage.update(id, newModel)
      } yield trainState
    }
  }

  /**
    * Check if the trained octopus is consistent.
    * This means that the alignment directory is available, lobster is consistent and that the SSDs
    * have not been updated since the octopus was last modified.
    *
    * @param id ID for the octopus
    * @return boolean
    */
  def isConsistent(id: OctopusID): Boolean = {
    logger.info(s"Checking consistency of octopus $id")

    // make sure the SSDs in the octopus are older
    // than the training state
    val isOK = for {
      octopus <- get(id)
      path = octopus.alignmentDir
      trainDate = octopus.state.dateChanged
      refIDs = octopus.ssds
      refs = refIDs.flatMap(SSDStorage.get).map(_.dateModified)

      // associated schema matcher model is consistent
      lobsterConsistent = ModelStorage.isConsistent(octopus.lobsterID)
      // make sure the octopus is complete
      isComplete = octopus.state.status == COMPLETE
      // make sure the SSDs are older than the training date
      allBefore = refs.forall(_.isBefore(trainDate))
      // make sure the alignment graph is there...
      alignmentExists = path.exists(Files.exists(_))

    } yield allBefore && alignmentExists && isComplete && lobsterConsistent

    isOK getOrElse false
  }
}
