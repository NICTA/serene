package com.nicta.dataint.matcher.eval.datasetutils

import com.nicta.dataint.data._
import com.nicta.dataint.ingestion.loader._
import com.nicta.dataint.matcher.eval._

import scala.util._

trait OpenGovCityCouncilUtils {
    val folder = "src/test/resources/datasets/open-gov-data/CityCouncilData/data"

    def loadDataSets(): List[DataModel] = {
        List("BallaratCity", "BrisbaneCityCouncil", "DPCDVic", "GreaterGeelongCity", "MosmanMunicipalCouncil").map({d =>
            CSVDataLoader(d).load(s"$folder/$d")
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