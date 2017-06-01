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

/**
 *  For each element in queryInstances,
  *  this class finds the minimum edit distance from all examples within each class.
 **/
object MinEditDistFromClassExamplesFeatureExtractor {
    def getGroupName(): String = "min-editdistance-from-class-examples"
}
case class MinEditDistFromClassExamplesFeatureExtractor(classList: List[String],
                                                        classExamplesMap: Map[String,List[String]]
                                                       ) extends HeaderGroupFeatureExtractor {
    val nameRegex = "([^@]+)@(.+)".r

    override def getGroupName(): String =
      MinEditDistFromClassExamplesFeatureExtractor.getGroupName

    override def getFeatureNames(): List[String] =
      classList.map {
        className => s"min-editdistance-$className"
      }

  val distMetric = (StringDistanceScaledByTotalLength())(
    OntoSimDistanceMetrics.computeDistance("NeedlemanWunschDistance")
  )

  override def computeSimpleFeatures(attribute: SimpleAttribute): List[Double] = {
    val attrName = attribute.metaName
      .getOrElse("") match {
      case nameRegex(name, _) => name
      case x => x
    }

    classList.map {
      className =>
        classExamplesMap.get(className).map {
          _.map {
            case nameRegex(name, _) => distMetric(attrName, name)
            case x => distMetric(attrName, x)
          }.min }.getOrElse(Double.MaxValue)
    }
  }

  override def computeFeatures(attribute: PreprocessedAttribute): List[Double] = {

    val attrName = attribute.rawAttribute.metadata
      .map(_.name)
      .getOrElse(attribute.rawAttribute.id) match {
        case nameRegex(name, _) => name
        case x => x
    }

    classList.map {
      className =>
        classExamplesMap.get(className).map {
          _.map {
            case nameRegex(name, _) => distMetric(attrName, name)
            case x => distMetric(attrName, x)
          }.min }.getOrElse(Double.MaxValue)
    }
  }
}