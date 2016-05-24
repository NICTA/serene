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

import DataSetTypes._

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
object MatcherInterface {
  val MissingValue = "unknown"

  /**
   * Parses a servlet request to get a dataset object
   * then adds to the database, and returns the case class response
   * object.
   *
   * @param request Servlet POST request
   * @return Case class object for JSON conversion
   */
  def createDataset(request: DataSetRequest): DataSet = {

    val fileStream = request.file getOrElse (throw new ParseException(s"Failed to read file request part: ${DataSetParser.FilePartName}"))
    val typeMap = request.typeMap getOrElse Map.empty[String, String]
    val description = request.description getOrElse MissingValue

    StorageLayer.addDataset(fileStream, description, typeMap)
  }

  def datasetKeys: List[DataSetID] = {
    StorageLayer.datasets.keys.toList
  }

  /**
   * Returns the public facing dataset from the storage layer
   *
   * @param id The dataset id
   * @return
   */
  def getDataSet(id: DataSetID): Option[DataSet] = {
    StorageLayer.datasets.get(id)
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
  def updateDataset(description: Option[String], typeMap: Option[TypeMap], key: DataSetID): DataSet = {

    if (!StorageLayer.datasets.contains(key)) {
      throw new ParseException(s"Dataset $key does not exist")
    }

    description foreach {
      StorageLayer.updateDescription(key, _)
    }

    typeMap foreach {
      StorageLayer.updateTypeMap(key, _)
    }

    StorageLayer.datasets.get(key) match {
      case Some(dataset) =>
        dataset
      case _ =>
        throw new Exception(s"Failed to update dataset $key")
    }
  }

  /**
   * Deletes the data set
   *
   * @param key Key for the dataset
   * @return
   */
  def deleteDataset(key: DataSetID): Option[DataSetID] = {
    StorageLayer.deleteDataset(key)
  }

}