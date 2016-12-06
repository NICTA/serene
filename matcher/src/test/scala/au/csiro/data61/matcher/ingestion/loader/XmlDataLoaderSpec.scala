package au.csiro.data61.matcher.ingestion.loader

import org.specs2._
import au.csiro.data61.matcher.data._
import au.csiro.data61.matcher.ingestion.loader._

class XmlDataLoaderSpec extends mutable.Specification {
    val homeseekersPath = "src/test/resources/datasets/wisc/realestate1/homeseekers"
    val texasPath = "src/test/resources/datasets/wisc/realestate1/Texas"

    s"""XmlDataLoaderSpec load("$homeseekersPath")""" should {
        s"load and parse xml data from $homeseekersPath" in {
            val data = XmlDataLoader("homeseekers","homeseekers","WISC - Homeseekers RealEstate1 Dataset").load(homeseekersPath)
            val numAttrInstances = data.children.get.map({_.asInstanceOf[Attribute].values.size})
            numAttrInstances.distinct.size mustEqual 1
            numAttrInstances.distinct.head mustEqual 2367
        }
    }

    s"""XmlDataLoaderSpec load("$texasPath")""" should {
        s"load and parse xml data from $texasPath" in {
            val data = XmlDataLoader("texas","texas","WISC - Texas RealEstate1 Dataset").load(texasPath)
            val numAttrInstances = data.children.get.map({_.asInstanceOf[Attribute].values.size})
            numAttrInstances.distinct.size mustEqual 1
            numAttrInstances.distinct.head mustEqual 2157
        }
    }
}