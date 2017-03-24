package au.csiro.data61.matcher.matcher.eval.datasetutils

import au.csiro.data61.matcher.data._
import au.csiro.data61.matcher.ingestion.loader._
import au.csiro.data61.matcher.matcher.eval._

import scala.util._

trait OpenGovCityCouncilUtils {
    val folder = "src/test/resources/datasets/open-gov-data/CityCouncilData/data"

    def loadDataSets(): List[DataModel] = {
        List("BallaratCity", "BrisbaneCityCouncil", "DPCDVic", "GreaterGeelongCity", "MosmanMunicipalCouncil").map({d =>
            CsvDataLoader(d).load(s"$folder/$d")
        })
        
    }

    def loadSemanticTypeLabels() = {
        // val gtPath = "src/test/resources/datasets/wisc/realestate1/common-fields.csv"
        // SemanticTypeLabelsLoader().load(gtPath)
        SemanticTypeLabels(Map())
    }

    def loadLabels(): Labels = {
        BasicLabels(List())
    }
}