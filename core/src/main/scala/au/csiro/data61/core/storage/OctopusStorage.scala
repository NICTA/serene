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

import java.io.{FileInputStream, File}
import java.nio.file.Path
import au.csiro.data61.core.Serene
import au.csiro.data61.core.types.ModelerTypes.{OctopusID, Octopus}
import au.csiro.data61.core.types.{TrainState, Status}
import org.joda.time.DateTime
import org.json4s.jackson.JsonMethods._

/**
  * OctopusStorage holds the Octopus objects in a key-value store
  */
object OctopusStorage extends Storage[OctopusID, Octopus] {

  override implicit val keyReader: Readable[Int] = Readable.ReadableInt

  override def rootDir: String = new File(Serene.config.storageDirs.octopus).getAbsolutePath

  def extract(stream: FileInputStream): Octopus = {
    parse(stream).extract[Octopus]
  }

  /**
    * updates the training state of octopus `id`
    *
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
        octopus <- get(id)
        // state dates should not be changed if changeDate is false
        trainState = TrainState(status, msg, DateTime.now)
        // we now update the octopus with the training information...
        newOctopus = octopus.copy(
          state = trainState,
          dateModified = octopus.dateModified
        )
        id <- OctopusStorage.update(id, newOctopus)
      } yield trainState
    }
  }

  def isConsistent(id: OctopusID): Boolean = {
    // TODO: Make this real!!
    true
  }
}
