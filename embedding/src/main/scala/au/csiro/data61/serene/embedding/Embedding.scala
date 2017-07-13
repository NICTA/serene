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
package au.csiro.data61.serene.embedding

object Matrix {
  type Matrix = Array[Array[Double]]
}

import Matrix._
import org.apache.spark.sql.DataFrame

sealed trait Embedding[T] {
  val matrix: T
}

/**
  * A vanilla node embedding
  *
  * @param matrix The vectors in the Vanilla vector space
  */
case class NodeEmbedding(matrix: Matrix) extends Embedding[Matrix]

/**
  * A meta-path node embedding
  *
  * @param matrix The vectors in the MetaPath vector space
  */
case class MetaPathEmbedding(matrix: Matrix) extends Embedding[Matrix]


object Embedder {

  def simple(df: DataFrame): NodeEmbedding = {
    NodeEmbedding(Array(Array(0.0)))
  }

  def metaPath(df: DataFrame): MetaPathEmbedding = {
    MetaPathEmbedding(Array(Array(0.0)))
  }
}