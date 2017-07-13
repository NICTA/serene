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
package au.csiro.data61.serene.integrator

/**
  * Sources describe the csv, json, database etc
  */
sealed trait Source
case class CSVSource(filename: String, columns: List[(String, String)]) extends Source

/**
  * Graph elements describe the Graph schema and the mapping from source to schema
  */
case class Class(fields: List[String])
case class Schema(classes: Set[Class])
case class Instance(schema: Schema, classes: List[Class])

/**
  * An SSD consists of a source description, a schema and a mapping.
  * @param source The definition of a data source e.g. columns and types
  * @param schema The definition of the graph schema
  * @param instance The mapping between the source and the schema
  */
case class Integrator(source: Option[Source],
                      schema: Option[Schema],
                      instance: Option[Instance]) {

}
