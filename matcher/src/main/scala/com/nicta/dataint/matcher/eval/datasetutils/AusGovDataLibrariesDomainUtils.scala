package com.nicta.dataint.matcher.eval.datasetutils

import com.nicta.dataint.data._
import com.nicta.dataint.ingestion.loader._
import com.nicta.dataint.matcher.eval._

import scala.util._

trait AusGovDataLibrariesDomainUtils {
    val dataPath = "src/test/resources/datasets/open-gov-data/Libraries/AU"
    val gtPath = "src/test/resources/datasets/open-gov-data/Libraries/AU_labels.txt"

    def loadDataSets(): List[DataModel] = {
        val tableNameRegex = """^(.+).csv""".r
        val filesList = new java.io.File(dataPath).list.filter({
            case tableNameRegex(a) => true
            case _ => false
        }).map({
            case tableNameRegex(a) => a + ".csv"
        })

        filesList.map({case f => {
            CSVDataLoader("AU").loadTable(dataPath, f, "AU")
        }}).toList
    }

    def loadLabels(): Labels = {
        BasicLabels(CsvLabelsLoader().loadLabels(gtPath).toList)
    }

    def partitionDataSet(datasets: List[DataModel], labels: Labels, propTrain: Double, randNumGenerator: Random = new Random(1000)): ((List[DataModel],Labels), (List[DataModel],Labels)) = {
        DataPartitioner().partitionByDataSet(datasets, labels, propTrain)
    }
}