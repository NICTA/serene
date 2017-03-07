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

import au.csiro.data61.core.api.InternalException
import au.csiro.data61.core.storage.Storage
import au.csiro.data61.types.Identifiable

import language.higherKinds


trait StorageInterface[Key <: Int, SereneResource <: Identifiable[Key]] {

  type StorageDependencyMap = Map[Class[_ <: Identifiable[Key]], List[Key]]

  protected val storage: Storage[Key, SereneResource]

  protected def add[T <: SereneResource](resource : T): Option[Key] = {
    val refs = missingReferences(resource)
    if (refs.isEmpty) {
      storage.add(resource.id, resource)
    } else {
      throw InternalException(s"References broken for add: ${beautify(refs)}")
    }
  }

  protected def update[T <: SereneResource](resource : T): Option[Key] = {
    val refs = missingReferences(resource)
    if (refs.isEmpty) {
      storage.update(resource.id, resource)
    } else {
      throw InternalException(s"References broken for update: ${beautify(refs)}")
    }
  }

  protected def remove[T <: SereneResource](resource: T): Option[Key] = {
    val refs = dependents(resource)

    if (refs.isEmpty) {
      storage.remove(resource.id)
    } else {
      throw InternalException(s"References broken for update: ${beautify(refs)}")
    }
  }

  protected def beautify(m: StorageDependencyMap): String = {
    m.map{
      case (k, v) =>
        k.getClass.getSimpleName -> v
    }.toString
  }

  protected def missingReferences[T <: SereneResource](resource: T): StorageDependencyMap

  protected def dependents[T <: SereneResource](resource: T): StorageDependencyMap
}

trait Trainable {
  def checkTraining: Boolean
}
