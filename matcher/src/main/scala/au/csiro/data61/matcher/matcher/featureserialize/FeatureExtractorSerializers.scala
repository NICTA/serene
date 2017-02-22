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
package au.csiro.data61.matcher.matcher.featureserialize

import au.csiro.data61.matcher.matcher.RfKnnFeature
import au.csiro.data61.matcher.matcher.features.ExampleBasedFeatureExtractorTypes.TokenizedWords
import au.csiro.data61.matcher.matcher.features._
import org.json4s._

import scala.util.{Failure, Success, Try}

object HelperJSON {
  implicit val formats = DefaultFormats + RfKnnFeatureExtractorSerializer +
    RfKnnFeatureSerializer +
    MinEditDistFromClassExamplesFeatureExtractorSerializer +
    JCNMinWordNetDistFromClassExamplesFeatureExtractorSerializer +
    LINMinWordNetDistFromClassExamplesFeatureExtractorSerializer +
    MeanCharacterCosineSimilarityFeatureExtractorSerializer +
    NamedGroupFeatureExtractorSerializer +
    ModelFeatureExtractorsSerializer +
    SingleFeatureExtractorSerializer
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


case class NamedGroupFeatureExtractor(name: String, groupFeatureExtractor: GroupFeatureExtractor)

case object  NamedGroupFeatureExtractorSerializer extends CustomSerializer[NamedGroupFeatureExtractor](format => (
  {
    case jv: JValue =>
      implicit val formats = DefaultFormats
      val name: String = (jv \ "groupFeatureName").extract[String]
      val groupFeatureExtractor: GroupFeatureExtractor = name match {
        case "prop-instances-per-class-in-knearestneighbours" =>
          HelperJSON.parseOption[RfKnnFeatureExtractor]("setting",jv)
          match {
            case Success(Some(fList)) => fList
            case Success(None) =>
              throw new Exception("Failed to extract RfKnnFeatureExctractor. Missing setting.")
            case Failure(err) =>
              throw new Exception(s"Failed to extract RfKnnFeatureExctractor: $err")
          }
        case  "min-editdistance-from-class-examples" =>
          HelperJSON.parseOption[MinEditDistFromClassExamplesFeatureExtractor]("setting",jv)
          match {
            case Success(Some(fList)) => fList
            case Success(None) =>
              throw new Exception("Failed to extract MinEditDistFromClassExamplesFeatureExtractor. Missing setting.")
            case Failure(err) =>
              throw new Exception(s"Failed to extract MinEditDistFromClassExamplesFeatureExtractor: $err")
          }
        case "min-wordnet-jcn-distance-from-class-examples" =>
          HelperJSON.parseOption[JCNMinWordNetDistFromClassExamplesFeatureExtractor]("setting",jv)
          match {
            case Success(Some(fList)) => fList
            case Success(None) =>
              throw new Exception("Failed to extract JCNMinWordNetDistFromClassExamplesFeatureExtractor. Missing setting.")
            case Failure(err) =>
              throw new Exception(s"Failed to extract JCNMinWordNetDistFromClassExamplesFeatureExtractor: $err")
          }
        case "min-wordnet-lin-distance-from-class-examples" =>
          HelperJSON.parseOption[LINMinWordNetDistFromClassExamplesFeatureExtractor]("setting",jv)
          match {
            case Success(Some(fList)) => fList
            case Success(None) =>
              throw new Exception("Failed to extract LINMinWordNetDistFromClassExamplesFeatureExtractor. Missing setting.")
            case Failure(err) =>
              throw new Exception(s"Failed to extract LINMinWordNetDistFromClassExamplesFeatureExtractor: $err")
          }
        case "mean-character-cosine-similarity-from-class-examples" =>
          HelperJSON.parseOption[MeanCharacterCosineSimilarityFeatureExtractor]("setting",jv)
          match {
            case Success(Some(fList)) => fList
            case Success(None) =>
              throw new Exception("Failed to extract MeanCharacterCosineSimilarityFeatureExtractor. Missing setting.")
            case Failure(err) =>
              throw new Exception(s"Failed to extract MeanCharacterCosineSimilarityFeatureExtractor: $err")
          }
        case "stats-of-text-length" =>
          TextStatsFeatureExtractor()
        case "stats-of-numerical-type" =>
          NumberTypeStatsFeatureExtractor()
        case "char-dist-features" =>
          CharDistFeatureExtractor()
        case _ =>
          throw new Exception(s"Failed to extract GroupFeatureExtractor. This one is not supported: $name")
      }
      NamedGroupFeatureExtractor(name, groupFeatureExtractor)
  }, {
  case feature: NamedGroupFeatureExtractor =>
    implicit val formats = DefaultFormats +
      RfKnnFeatureExtractorSerializer +
      MinEditDistFromClassExamplesFeatureExtractorSerializer +
      JCNMinWordNetDistFromClassExamplesFeatureExtractorSerializer +
      LINMinWordNetDistFromClassExamplesFeatureExtractorSerializer +
      MeanCharacterCosineSimilarityFeatureExtractorSerializer

    feature.name match {
      case "stats-of-text-length" =>
        JObject (JField ("groupFeatureName", JString (feature.name) ) :: Nil)
      case "stats-of-numerical-type" =>
        JObject (JField ("groupFeatureName", JString (feature.name) ) :: Nil)
      case "char-dist-features" =>
        JObject (JField ("groupFeatureName", JString (feature.name) ) :: Nil)
      case _ =>
      val jFeatureExtractor = Extraction.decompose(feature.groupFeatureExtractor)
      JObject (
        JField ("groupFeatureName", JString (feature.name) ) ::
          JField ("setting", jFeatureExtractor) :: Nil
      )
    }
}))

/**
  * Case class to store feature extractors of the model.
  * @param featureExtractors
  */
case class ModelFeatureExtractors(featureExtractors: List[FeatureExtractor])

case object ModelFeatureExtractorsSerializer extends CustomSerializer[ModelFeatureExtractors](format => (
  {
    case jv: JValue =>
      implicit val formats = DefaultFormats
      val singleFeatures: List[SingleFeatureExtractor] = HelperJSON
        .parseOption[List[SingleFeatureExtractor]]("singleFeatureExtractors",jv) match {
        case Success(Some(fList)) => fList
        case Success(None) => List()
        case Failure(err) =>
          throw new Exception(s"Failed to extract SingleFeatureExtractors: $err")
      }
      val groupFeatures: List[GroupFeatureExtractor] = HelperJSON
        .parseOption[List[NamedGroupFeatureExtractor]]("groupFeatureExtractors",jv) match {
        case Success(Some(fList)) =>
          fList.map {_.groupFeatureExtractor}
        case Success(None) =>
          List()
        case Failure(err) =>
          throw new Exception(s"Failed to extract Group Feature Extractors: ${err}")
      }
      ModelFeatureExtractors(singleFeatures ++ groupFeatures)

  }, {
  case feature: ModelFeatureExtractors =>
    implicit val formats = DefaultFormats +
      NamedGroupFeatureExtractorSerializer +
      SingleFeatureExtractorSerializer

    val singleFeatures: List[SingleFeatureExtractor] = feature.featureExtractors
      .flatMap {
        case x: SingleFeatureExtractor => List(x)
        case _ => None
      }
    val groupFeatures: List[NamedGroupFeatureExtractor] = feature.featureExtractors
      .flatMap {
        case x: GroupFeatureExtractor =>
          List(NamedGroupFeatureExtractor(x.getGroupName, x))
        case _ => None
    }

    val jSingle = Extraction.decompose(singleFeatures)
    val jGroup = Extraction.decompose(groupFeatures)

    JObject(
      JField("singleFeatureExtractors" , jSingle) ::
        JField("groupFeatureExtractors" , jGroup) :: Nil
    )
}))



//JSON serializers for GroupExtractorFeatures

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
      implicit val formats = DefaultFormats + RfKnnFeatureSerializer
      val k: Int = HelperJSON.parseOption[Int]("parameter",jv)
      match {
        case Success(Some(fList)) => fList
        case _ =>
          throw new Exception("Failed to extract parameter for RfKnnFeatureExtractor")
      }

      val classList: List[String] = (jv \ "classList").extract[List[String]]

      val pool: List[RfKnnFeature] = HelperJSON.parseOption[List[RfKnnFeature]]("pool",jv)
      match {
        case Success(Some(fList)) => fList
        case _ =>
          throw new Exception("Failed to extract RfKnnFeature")
      }

      RfKnnFeatureExtractor(classList, pool, k)
  }, {
  case featureExtractor: RfKnnFeatureExtractor =>
    implicit val formats = DefaultFormats + RfKnnFeatureSerializer
    val pool = Extraction.decompose(featureExtractor.pool)
    val jvClass = Extraction.decompose(featureExtractor.classList)
    JObject(JField("classList", jvClass) ::
      JField("pool",pool) ::
      JField("parameter", JInt(featureExtractor.k)) :: Nil
    )

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
        case _ => throw new Exception("Failed to extract MinEditDistFromClassExamplesFeatureExtractor")
      }
      MinEditDistFromClassExamplesFeatureExtractor(classList, classExamplesMap)
  }, {
  case featureExtractor: MinEditDistFromClassExamplesFeatureExtractor =>
    implicit val formats = DefaultFormats + RfKnnFeatureSerializer
    val pool = Extraction.decompose(featureExtractor.classExamplesMap)
    val jvClass = Extraction.decompose(featureExtractor.classList)
    JObject(JField("classList", jvClass) ::
      JField("classExamplesMap",pool) :: Nil)

}))

case object JCNMinWordNetDistFromClassExamplesFeatureExtractorSerializer
  extends CustomSerializer[JCNMinWordNetDistFromClassExamplesFeatureExtractor](format => (
    {
      case jv: JValue =>
        implicit val formats = DefaultFormats
        val classList: List[String] = (jv \ "classList").extract[List[String]]
        val pool: Map[String,List[TokenizedWords]] = HelperJSON
          .parseOption[Map[String,List[TokenizedWords]]]("pool",jv) match {
          case Success(Some(fList)) => fList
          case _ => throw new Exception("Failed to extract JCNMinWordNetDistFromClassExamplesFeatureExtractor")
        }
        val k: Int = HelperJSON.parseOption[Int]("parameter",jv)
        match {
          case Success(Some(fList)) => fList
          case _ =>
            throw new Exception("Failed to extract parameter for JCNMinWordNetDistFromClassExamplesFeatureExtractor")
        }
        JCNMinWordNetDistFromClassExamplesFeatureExtractor(classList, pool, k)
    }, {
    case featureExtractor: JCNMinWordNetDistFromClassExamplesFeatureExtractor =>
      implicit val formats = DefaultFormats + RfKnnFeatureSerializer
      val pool = Extraction.decompose(featureExtractor.pool)
      val jvClass = Extraction.decompose(featureExtractor.classList)
      JObject(JField("classList", jvClass) ::
        JField("pool",pool) ::
        JField("parameter",JInt(featureExtractor.maxComparisons)) :: Nil)

  }))

case object LINMinWordNetDistFromClassExamplesFeatureExtractorSerializer
  extends CustomSerializer[LINMinWordNetDistFromClassExamplesFeatureExtractor](format => (
    {
      case jv: JValue =>
        implicit val formats = DefaultFormats
        val classList: List[String] = (jv \ "classList").extract[List[String]]
        val pool: Map[String,List[TokenizedWords]] = HelperJSON
          .parseOption[Map[String,List[TokenizedWords]]]("pool",jv) match {
          case Success(Some(fList)) => fList
          case _ => throw new Exception("Failed to extract LINMinWordNetDistFromClassExamplesFeatureExtractor")
        }
        val k: Int = HelperJSON.parseOption[Int]("parameter",jv)
        match {
          case Success(Some(fList)) => fList
          case _ =>
            throw new Exception("Failed to extract parameter for LINMinWordNetDistFromClassExamplesFeatureExtractor")
        }
        LINMinWordNetDistFromClassExamplesFeatureExtractor(classList, pool, k)
    }, {
    case featureExtractor: LINMinWordNetDistFromClassExamplesFeatureExtractor =>
      implicit val formats = DefaultFormats + RfKnnFeatureSerializer
      val pool = Extraction.decompose(featureExtractor.pool)
      val jvClass = Extraction.decompose(featureExtractor.classList)
      JObject(JField("classList", jvClass) ::
        JField("pool",pool) ::
        JField("parameter", JInt(featureExtractor.maxComparisons)) :: Nil
      )
  }))

case object MeanCharacterCosineSimilarityFeatureExtractorSerializer
  extends CustomSerializer[MeanCharacterCosineSimilarityFeatureExtractor](format => (
    {
      case jv: JValue =>
        implicit val formats = DefaultFormats
        val classList: List[String] = (jv \ "classList").extract[List[String]]
        val classExamplesMap: Map[String,List[Map[Char,Double]]] = HelperJSON
          .parseOption[Map[String,List[Map[String,Double]]]]("classExamplesMap",jv) match {
          case Success(Some(fList)) => fList.map {
            case (k,v)  =>
              (k, v.map {x => x.map {case (k2,v2) => (k2.head, v2)}})
          }
          case _ => throw new Exception("Failed to extract MeanCharacterCosineSimilarityFeatureExtractor")
        }
        MeanCharacterCosineSimilarityFeatureExtractor(classList, classExamplesMap)
    }, {
    case featureExtractor: MeanCharacterCosineSimilarityFeatureExtractor =>
      implicit val formats = DefaultFormats + RfKnnFeatureSerializer
      // we need to convert Char to String since json does not support char
      val classExamplesMap: Map[String,List[Map[String,Double]]] =
        featureExtractor.classExamplesMap.map {
          case (k,v)  =>
            (k, v.map {x => x.map {case (k2,v2) => (k2.toString, v2)}})
        }
      val pool = Extraction.decompose(classExamplesMap)
      val jvClass = Extraction.decompose(featureExtractor.classList)
      JObject(JField("classList", jvClass) ::
        JField("classExamplesMap",pool) :: Nil
      )

  }))

case object SingleFeatureExtractorSerializer extends CustomSerializer[SingleFeatureExtractor](format => (
  {
    case jv: JValue =>
      implicit val formats = DefaultFormats
      val name: String = (jv \ "singleFeatureName").extract[String]
      val featureExtractor: SingleFeatureExtractor = name match {
        case "num-unique-vals" =>
          NumUniqueValuesFeatureExtractor().asInstanceOf[SingleFeatureExtractor]
        case "prop-unique-vals" =>
          PropUniqueValuesFeatureExtractor().asInstanceOf[SingleFeatureExtractor]
        case "prop-missing-vals" =>
          PropMissingValuesFeatureExtractor().asInstanceOf[SingleFeatureExtractor]
        case "prop-numerical-chars" =>
          NumericalCharRatioFeatureExtractor().asInstanceOf[SingleFeatureExtractor]
        case "prop-whitespace-chars" =>
          WhitespaceRatioFeatureExtractor().asInstanceOf[SingleFeatureExtractor]
        case "is-discrete" =>
          DiscreteTypeFeatureExtractor().asInstanceOf[SingleFeatureExtractor]
        case "entropy-for-discrete-values" =>
          EntropyForDiscreteDataFeatureExtractor().asInstanceOf[SingleFeatureExtractor]
        case "ratio-alpha-chars" =>
          PropAlphaCharsFeatureExtractor().asInstanceOf[SingleFeatureExtractor]
        case "prop-entries-with-at-sign" =>
          PropEntriesWithAtSign().asInstanceOf[SingleFeatureExtractor]
        case "prop-entries-with-currency-symbol" =>
          PropEntriesWithCurrencySymbol().asInstanceOf[SingleFeatureExtractor]
        case "prop-entries-with-hyphen" =>
          PropEntriesWithHyphen().asInstanceOf[SingleFeatureExtractor]
        case "prop-entries-with-paren" =>
          PropEntriesWithParen().asInstanceOf[SingleFeatureExtractor]
        case "mean-commas-per-entry" =>
          MeanCommasPerEntry().asInstanceOf[SingleFeatureExtractor]
        case "mean-forward-slashes-per-entry" =>
          MeanForwardSlashesPerEntry().asInstanceOf[SingleFeatureExtractor]
        case "prop-range-format" =>
          PropRangeFormat().asInstanceOf[SingleFeatureExtractor]
        case "prop-datepattern" =>
          DatePatternFeatureExtractor().asInstanceOf[SingleFeatureExtractor]
        case _ =>
          throw new Exception(s"Failed to extract SingleFeatureExtractor. FeatureExtractor $name is not supported")
      }
      featureExtractor
  }, {
  case feature: SingleFeatureExtractor =>
    implicit val formats = DefaultFormats
    JObject(
      JField("singleFeatureName" , JString(feature.getFeatureName)) :: Nil
    )
}))

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

