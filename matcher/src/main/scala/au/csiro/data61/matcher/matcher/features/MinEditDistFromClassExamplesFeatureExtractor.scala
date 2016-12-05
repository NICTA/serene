package au.csiro.data61.matcher.matcher.features

import au.csiro.data61.matcher.data._
import au.csiro.data61.matcher.matcher._
import au.csiro.data61.matcher.nlptools.distance._

/**
 *  For each element in queryInstances, this class finds the minimum edit distance from all examples within each class.
 **/
object MinEditDistFromClassExamplesFeatureExtractor {
    def getGroupName() = "min-editdistance-from-class-examples"
}
case class MinEditDistFromClassExamplesFeatureExtractor(classList: List[String], classExamplesMap: Map[String,List[String]]) extends GroupFeatureExtractor {
    val nameRegex = "([^@]+)@(.+)".r

    override def getGroupName() = MinEditDistFromClassExamplesFeatureExtractor.getGroupName
    override def getFeatureNames(): List[String] = classList.map({className => s"min-editdistance-$className"}).toList
    val distMetric = (StringDistanceScaledByTotalLength())(OntoSimDistanceMetrics.computeDistance("NeedlemanWunschDistance"))
    
    override def computeFeatures(attribute: PreprocessedAttribute): List[Double] = {
        val attrname = attribute.rawAttribute.metadata.map({_.name}).getOrElse(attribute.rawAttribute.id) match {
            case nameRegex(name, _) => name
            case x => x
        }

        classList.map({className => 
            classExamplesMap.get(className).map({_.map({
                case nameRegex(name, _) => name
                case x => x
            }).map({case exampleName => distMetric(attrname, exampleName)}).min}).getOrElse(Double.MaxValue)
        }).toList
    }
}