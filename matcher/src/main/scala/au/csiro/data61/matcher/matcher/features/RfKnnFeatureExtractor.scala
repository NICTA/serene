package au.csiro.data61.matcher.matcher.features

import au.csiro.data61.matcher.data._
import au.csiro.data61.matcher.matcher._
import au.csiro.data61.matcher.nlptools.distance._

import edu.cmu.lti.ws4j._

case class StringDistanceScaledByTotalLength() extends Function[(String, String) => Double, (String, String) => Double] {
    def apply(distfunc: (String, String) => Double): (String, String) => Double = {
        (s1: String, s2: String) => {
            val dist = distfunc(s1,s2)
            dist
            // dist / (s1.length + s2.length).toDouble
        }
    }
}


/**
 *  For each element in queryInstances, this class finds the k nearest neighbours from pool.
 **/
object RfKnnFeatureExtractor {
    def getGroupName() = "prop-instances-per-class-in-knearestneighbours" 
}
case class RfKnnFeatureExtractor(classList: List[String], pool: List[RfKnnFeature], k: Int) extends GroupFeatureExtractor {
    val nameRegex = "([^@]+)@(.+)".r

    override def getGroupName() = RfKnnFeatureExtractor.getGroupName
    override def getFeatureNames(): List[String] = classList.map({className => s"prop-$className"}).toList
    val distMetric = (StringDistanceScaledByTotalLength())(OntoSimDistanceMetrics.computeDistance("NeedlemanWunschDistance"))
    val distMatrix = Map[String, Map[String, Double]]()

    override def computeFeatures(attribute: PreprocessedAttribute): List[Double] = {
        attribute.rawAttribute.metadata.map({metadata =>
            val neighbourLabels = findNearestNeighbourLabels(attribute.rawAttribute.id, metadata.name, k)
            classList.map({className =>
                neighbourLabels.filter({_ == className}).size.toDouble / neighbourLabels.size.toDouble
            }).toList
        }).getOrElse({
            classList.map({x => -1.0}) //default to -1 if attribute does not have a name
        })
    }

    def findNearestNeighbourLabels(attrId: String, attributeName: String, k: Int): List[String] = {
        val attrNameTruncated = attributeName match {
            case nameRegex(name, _) => name
            case x => x
        }

        val sortedInstances = pool.filter({case inst => inst.attributeId != attrId}).map({poolInstance => 
            val instName = poolInstance.attributeName match {
                case nameRegex(name, _) => name
                case x => x
            }
            (distMetric(instName, attrNameTruncated), poolInstance, poolInstance.label)
        }).sortBy({_._1})

        sortedInstances.take(k).map({_._3}).toList
    }
}