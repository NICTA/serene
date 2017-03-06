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

import au.csiro.data61.core.storage.SsdStorage
import au.csiro.data61.types.Ssd
import au.csiro.data61.types.SsdTypes.SsdID
import com.typesafe.scalalogging.LazyLogging

object SsdInterface extends StorageInterface[SsdID, Ssd] with LazyLogging {

  override val storage = SsdStorage

  protected def missingReferences[Ssd](resource: Ssd): StorageDependencyMap = Map()

  protected def dependents[Ssd](resource: Ssd): StorageDependencyMap = Map()

}
