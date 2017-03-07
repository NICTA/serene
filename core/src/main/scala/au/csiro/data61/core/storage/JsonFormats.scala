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
package au.csiro.data61.core.storage

import au.csiro.data61.types.SsdTypes.OwlDocumentFormat
import au.csiro.data61.types.Training.StatusSerializer
import org.json4s.Formats
import java.nio.file.{Path, Paths}

import au.csiro.data61.types._
import au.csiro.data61.matcher.matcher.featureserialize._
import org.json4s._
import org.json4s.ext.EnumNameSerializer

/**
  * Serializer for the Java.io.Path object
  */
case object PathSerializer extends CustomSerializer[Path](format => ( {
  case jv: JValue =>
    implicit val formats = DefaultFormats
    val str = jv.extract[String]
    Paths.get(str)
}, {
  case path: Path =>
    JString(path.toString)
}))


/**
 * Holds the implicit matcher objects for the Json4s Serializers.
 *
 * This should be mixed in to the object in order to use.
 */
trait JsonFormats {

  implicit def json4sFormats: Formats =
    org.json4s.DefaultFormats +
      //org.json4s.ext.JodaTimeSerializers.all +
      JodaTimeSerializer +
      LogicalTypeSerializer +
      PathSerializer +
      SamplingStrategySerializer +
      ModelTypeSerializer +
      FeatureSerializer +
      StatusSerializer +
      FeaturesConfigSerializer +
      RfKnnFeatureExtractorSerializer +
      RfKnnFeatureSerializer +
      MinEditDistFromClassExamplesFeatureExtractorSerializer +
      JCNMinWordNetDistFromClassExamplesFeatureExtractorSerializer +
      LINMinWordNetDistFromClassExamplesFeatureExtractorSerializer +
      MeanCharacterCosineSimilarityFeatureExtractorSerializer +
      ModelFeatureExtractorsSerializer +
      SingleFeatureExtractorSerializer +
      NamedGroupFeatureExtractorSerializer +
      SsdNodeSerializer +
      HelperLinkSerializer +
      SemanticModelSerializer +
      SsdMappingSerializer +
      new EnumNameSerializer(OwlDocumentFormat)
}
