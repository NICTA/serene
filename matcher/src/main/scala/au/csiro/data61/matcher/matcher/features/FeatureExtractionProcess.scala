package com.nicta.dataint.matcher.features

import com.nicta.dataint.data._

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