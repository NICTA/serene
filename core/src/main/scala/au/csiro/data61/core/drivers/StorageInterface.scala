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

import javax.ws.rs.ext.ParamConverter.Lazy

import au.csiro.data61.core.api.{BadRequestException, InternalException}
import au.csiro.data61.core.storage._
import au.csiro.data61.types.DataSetTypes.DataSetID
import au.csiro.data61.types.Identifiable
import au.csiro.data61.types.ModelTypes.ModelID
import au.csiro.data61.types.SsdTypes.{OctopusID, OwlID, SsdID}
import com.typesafe.scalalogging.LazyLogging

import scala.util.{Failure, Random, Success, Try}
import language.higherKinds


/**
  * abstract type member for the keys used for our data structures
  */
sealed trait KeyType{
  type Key

}

trait SsdKey extends KeyType {
  type Key = SsdID
}

trait OwlKey extends KeyType{
  type Key = OwlID
}
trait OctopusKey extends KeyType{
  type Key = OctopusID
}
trait DatasetKey extends KeyType{
  type Key = DataSetID
}
trait ModelKey extends KeyType{
  type Key = ModelID
}

/**
  * An object which stores dependencies/references for a resource.
  *
  * @param column List of dependencies/references of type ColumnID
  * @param dataset List of dependencies/references of type DatasetID from the DatasetStorage
  * @param owl List of dependencies/references of type OwlID from the OwlStorage
  * @param ssd List of dependencies/references of type SsdID from the SsdStorage
  * @param model List of dependencies/references of type ModelID from the ModelStorage
  * @param octopus List of dependencies/references of type OctopusID from the OctopusStorage
  */
case class StorageDependencyMap(column: List[Int] = List.empty[Int],
                                dataset: List[Int] = List.empty[Int],
                                owl: List[Int] = List.empty[Int],
                                ssd: List[Int] = List.empty[Int],
                                model: List[Int] = List.empty[Int],
                                octopus: List[Int] = List.empty[Int]
                               ){
  def isEmpty: Boolean = {
    dataset.isEmpty && owl.isEmpty && ssd.isEmpty && model.isEmpty && octopus.isEmpty && column.isEmpty
  }

  override def toString: String = "columns: [" + column.mkString(",") +
    "], dataset: [" + dataset.mkString(",") +
    "], owl: [" + owl.mkString(",") +
    "], ssd: [" + dataset.mkString(",") +
    "], model: [" + model.mkString(",") +
    "], octopus: [" + dataset.mkString(",") + "]"
}

/**
  * Abstract interface for the storage.
  * @tparam K Type of the key used for the resource
  * @tparam SereneResource Type of the resource
  */
trait StorageInterface[K <: KeyType, SereneResource <: Identifiable[K#Key]] extends LazyLogging {

  type Key = K#Key

  protected val storage: Storage[Key, SereneResource]

  // we access it in some tests...
  val MissingValue = "unknown"

  /**
    * Passes the storage keys up to the API
    */
  def storageKeys: List[Key] = storage.keys

  def add(resource : SereneResource): Option[Key] = {
    val refs = missingReferences(resource)
    if (refs.isEmpty) {
      storage.add(resource.id, resource)
    } else {
      throw BadRequestException(s"References broken for add: ${beautify(refs)}")
    }
  }

  protected def update(resource : SereneResource): Option[Key] = {
    val refs = missingReferences(resource)
    if (refs.isEmpty) {
      logger.debug(s"References are ok for resource ${resource.id}")
      storage.update(resource.id, resource)
    } else {
      logger.error(s"References broken for update: ${beautify(refs)}")
      throw BadRequestException(s"References broken for update: ${beautify(refs)}")
    }
  }

  protected def remove(resource: SereneResource, force: Boolean = false): Option[Key] = {
    val refs = dependents(resource)

    if (refs.isEmpty) {
      storage.remove(resource.id)
    } else if (force) {
      logger.warn("Forceful deletion of the resource attempted!")
//      throw InternalException(s"Forceful deletion not implemented.")
      Try {
        // the order of deletion matters!
        refs.octopus.foreach(OctopusStorage.remove)
        refs.model.foreach(ModelStorage.remove)
        refs.ssd.foreach(SsdStorage.remove)
        refs.dataset.foreach(DatasetStorage.remove)
        refs.owl.foreach(OwlStorage.remove)
      } match {
        case Success(_) =>
          logger.info("Forceful deletion of the resource succeeded.")
          Some(resource.id)
        case Failure(err) =>
          logger.error(s"Forceful deletion of the resource failed: $err")
          throw InternalException(s"Forceful deletion of the resource failed: $err")
      }
    } else {
      throw BadRequestException(s"Deletion not possible due to dependents: ${beautify(refs)}")
    }
  }

  def delete(key: Key, force: Boolean = false): Option[Key] = {
    for {
      resource <- storage.get(key)
      id <- remove(resource)
    } yield id
  }

  def get(key: Key): Option[SereneResource] = {
    logger.debug(s"Getting resource $key")
    storage.get(key)
  }

  protected def beautify(m: StorageDependencyMap): String = {
//    m.map{
//      case (k, v) =>
//        k.getClass.getSimpleName -> v
//    }.toString
    m.toString
  }

  protected def missingReferences(resource: SereneResource): StorageDependencyMap

  protected def dependents(resource: SereneResource): StorageDependencyMap
}

trait TrainableInterface[K <: KeyType, SereneResource <: Identifiable[K#Key]] extends StorageInterface[K, SereneResource] {
  def checkTraining(key: Key): Boolean
}
