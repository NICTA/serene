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

package au.csiro.data61.matcher.matcher.features

import au.csiro.data61.matcher.data._
import au.csiro.data61.matcher.matcher._
import au.csiro.data61.matcher.nlptools.distance._

import edu.cmu.lti.ws4j._

case class StringDistanceScaledByTotalLength() extends Function[(String, String) => Double, (String, String) => Double] {

  def apply(distfunc: (String, String) => Double): (String, String) => Double = {
    (s1: String, s2: String) => distfunc(s1,s2)
      // dist / (s1.length + s2.length).toDouble
  }
}


/**
 *  For each element in queryInstances, this class finds the k nearest neighbours from pool.
 **/
object RfKnnFeatureExtractor {
    def getGroupName(): String = "prop-instances-per-class-in-knearestneighbours"
}
case class RfKnnFeatureExtractor(classList: List[String], pool: List[RfKnnFeature], k: Int)
  extends HeaderGroupFeatureExtractor {
  val nameRegex = "([^@]+)@(.+)".r

  override def getGroupName(): String =
    RfKnnFeatureExtractor.getGroupName()

  override def getFeatureNames(): List[String] =
    classList.map { className => s"prop-$className" }

    val distMetric = (StringDistanceScaledByTotalLength())(
      OntoSimDistanceMetrics.computeDistance("NeedlemanWunschDistance"))
    val distMatrix = Map[String, Map[String, Double]]()

  override def computeSimpleFeatures(attribute: SimpleAttribute): List[Double] = {
    attribute.metaName.map {
      metadata =>
        val neighbourLabels = findNearestNeighbourLabels(attribute.attributeName, metadata, k)
        classList.map {
          className =>
            neighbourLabels.count(_ == className).toDouble / neighbourLabels.size
        }
    }.getOrElse {
      classList.map(_ => -1.0) //default to -1 if attribute does not have a name
    }
  }

  override def computeFeatures(attribute: PreprocessedAttribute): List[Double] = {
    attribute.rawAttribute.metadata.map {
      metadata =>
        val neighbourLabels = findNearestNeighbourLabels(attribute.rawAttribute.id, metadata.name, k)
        classList.map {
          className =>
            neighbourLabels.count(_ == className).toDouble / neighbourLabels.size
        }
    }.getOrElse {
        classList.map(_ => -1.0) //default to -1 if attribute does not have a name
    }
  }

  def findNearestNeighbourLabels(attrId: String, attributeName: String, k: Int): List[String] = {
    val attrNameTruncated = attributeName match {
      case nameRegex(name, _) => name
      case x => x
    }

    val sortedInstances = pool.filter(_.attributeId != attrId)
      .map {
        poolInstance =>
          val instName = poolInstance.attributeName match {
            case nameRegex(name, _) => name
            case x => x
          }
          (distMetric(instName, attrNameTruncated), poolInstance, poolInstance.label)
      }.sortBy(_._1)

    sortedInstances.take(k).map(_._3)
  }
}