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
package au.csiro.data61.core.types

import java.nio.file.Path

import au.csiro.data61.matcher.matcher.RfKnnFeature
import au.csiro.data61.matcher.matcher.features.{MinEditDistFromClassExamplesFeatureExtractor, RfKnnFeatureExtractor}
import org.json4s._

import scala.util.{Failure, Success, Try}

object HelperJSON {
  implicit val formats = DefaultFormats + RfKnnFeatureSerializer
  /**
    * Helper function to parse json objects. This will return None if
    * nothing is present, and throw a BadRequest error if it is incorrect,
    * and Some(T) if correct
    *
    * @param label The key for the object. Must be present in jValue
    * @param jValue The Json Object
    * @tparam T The return type
    * @return
    */
  def parseOption[T: Manifest](label: String, jValue: JValue): Try[Option[T]] = {
    val jv = jValue \ label
    if (jv == JNothing) {
      Success(None)
    } else {
      Try {
        Some(jv.extract[T])
      } recoverWith {
        case err =>
          Failure(
            new Exception(s"Failed to parse: $label. Error: ${err.getMessage}"))
      }
    }
  }
}

//group

case object RfKnnFeatureSerializer extends CustomSerializer[RfKnnFeature](format => (
  {
    case jv: JValue =>
      implicit val formats = DefaultFormats
      val attributeId: String = (jv \ "attributeId").extract[String]
      val attributeName: String = (jv \ "attributeName").extract[String]
      val label: String = (jv \ "label").extract[String]
      RfKnnFeature(attributeId, attributeName, label)
  }, {
  case feature: RfKnnFeature =>
    implicit val formats = DefaultFormats
    JObject(
      JField("attributeId" , JString(feature.attributeId)) ::
      JField("attributeName" , JString(feature.attributeName)) ::
      JField("label" , JString(feature.label)) :: Nil
    )
}))

case object RfKnnFeatureExtractorSerializer extends CustomSerializer[RfKnnFeatureExtractor](format => (
  {
    case jv: JValue =>
      implicit val formats = DefaultFormats
      val classList: List[String] = (jv \ "classList").extract[List[String]]
      val pool: List[RfKnnFeature] = HelperJSON.parseOption[List[RfKnnFeature]]("pool",jv) match {
        case Success(Some(fList)) => fList
        case _ => throw new Exception("Failed to extract RfKnnFeature")
      }
      val k: Int = (jv \ "parameter").extract[Int]
      RfKnnFeatureExtractor(classList, pool, k)
  }, {
  case featureExtractor: RfKnnFeatureExtractor =>
    implicit val formats = DefaultFormats + RfKnnFeatureSerializer
    val pool = Extraction.decompose(featureExtractor.pool)
    val jvClass = JArray(featureExtractor.classList.map(JString))
    JObject(JField("classList", jvClass) ::
      JField("pool",pool) ::
      JField("parameter",JInt(featureExtractor.k)) :: Nil)

}))

case object MinEditDistFromClassExamplesFeatureExtractorSerializer
  extends CustomSerializer[MinEditDistFromClassExamplesFeatureExtractor](format => (
  {
    case jv: JValue =>
      implicit val formats = DefaultFormats
      val classList: List[String] = (jv \ "classList").extract[List[String]]
      val classExamplesMap: Map[String,List[String]] = HelperJSON
        .parseOption[Map[String,List[String]]]("classExamplesMap",jv) match {
        case Success(Some(fList)) => fList
        case _ => throw new Exception("Failed to extract RfKnnFeature")
      }
      MinEditDistFromClassExamplesFeatureExtractor(classList, classExamplesMap)
  }, {
  case featureExtractor: MinEditDistFromClassExamplesFeatureExtractor =>
    implicit val formats = DefaultFormats + RfKnnFeatureSerializer
    val pool = Extraction.decompose(featureExtractor.classExamplesMap)
    val jvClass = JArray(featureExtractor.classList.map(JString))
    JObject(JField("classList", jvClass) ::
      JField("pool",pool) :: Nil)

}))

//JCNMinWordNetDistFromClassExamplesFeatureExtractor(classes, auxData, maxComparisons)
//LINMinWordNetDistFromClassExamplesFeatureExtractor(classes, auxData, maxComparisons)
//MeanCharacterCosineSimilarityFeatureExtractor(classes, auxData)

// single
//NumUniqueValuesFeatureExtractor
//PropUniqueValuesFeatureExtractor
//PropMissingValuesFeatureExtractor
//NumericalCharRatioFeatureExtractor
//WhitespaceRatioFeatureExtractor
//DiscreteTypeFeatureExtractor
//EntropyForDiscreteDataFeatureExtractor
//PropAlphaCharsFeatureExtractor
//PropEntriesWithAtSign
//PropEntriesWithCurrencySymbol
//PropEntriesWithHyphen
//PropEntriesWithParen
//MeanCommasPerEntry
//MeanForwardSlashesPerEntry
//PropRangeFormat
//DatePatternFeatureExtractor
//DataTypeFeatureExtractor
//TextStatsFeatureExtractor
//NumberTypeStatsFeatureExtractor
//CharDistFeatureExtractor

/**
  * Holds the implicit matcher objects for the Json4s Serializers.
  *
  * This should be mixed in to the object in order to use.
  */
