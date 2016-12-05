package au.csiro.data61.matcher.matcher.features

import au.csiro.data61.matcher.data._

case class FeatureExtractionProcess() {
    def extractFeatures(extractors: List[FeatureExtractor], attributes: List[PreprocessedAttribute]): List[List[Any]] = {
        attributes.map({attribute =>
            extractors.flatMap({
                case extractor: SingleFeatureExtractor => List(extractor.computeFeature(attribute))
                case extractor: GroupFeatureExtractor => extractor.computeFeatures(attribute)
            }).toList
        }).toList
    }
}