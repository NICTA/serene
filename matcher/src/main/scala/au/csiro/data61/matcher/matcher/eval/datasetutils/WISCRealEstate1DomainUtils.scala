package com.nicta.dataint.matcher.eval.datasetutils

import com.nicta.dataint.data._
import com.nicta.dataint.ingestion.loader._
import com.nicta.dataint.matcher.eval._

import scala.util._

trait WISCRealEstateDomain1Utils {
    val folder = "src/test/resources/datasets/wisc/realestate1"
    val descTemplate: (String => String) = (x: String) => s"WISC - $x Dataset"

    def loadDataSets(): List[DataModel] = {
        val datasetPaths = List(
            (s"$folder/homeseekers", "homeseekers", descTemplate("Homeseekers"), None),
            (s"$folder/Texas", "texas", descTemplate("Texas"), None),
            (s"$folder/NKY", "nky", descTemplate("NKY"), None),
            (s"$folder/Windermere", "windermere", descTemplate("Windermere"), None),
            (s"$folder/yahoo", "yahoo", descTemplate("Yahoo!"), Some(Set("stories")))
        )

        datasetPaths.map({case (path,id,desc,exclSet) => XmlDataLoader(id,id,desc,exclSet).load(path)}).toList
    }

    def loadLabels(): Labels = {
        val gtPath = "src/test/resources/datasets/wisc/realestate1/mappings-combined.csv"
        val ambigLabelsPath = "src/test/resources/datasets/wisc/realestate1/ambiguous-mappings-combined.csv"
        PosAndAmbigLabelsLoader().load(gtPath,ambigLabelsPath)
    }

    def loadSemanticTypeLabels() = {
        val gtPath = "src/test/resources/datasets/wisc/realestate1/semtype_labels.csv"
        SemanticTypeLabelsLoader().load(gtPath)
    }

    def partitionDataSet(datasets: List[DataModel], labels: Labels, propTrain: Double, randNumGenerator: Random = new Random(1000)): ((List[DataModel],Labels), (List[DataModel],Labels)) = {
        DataPartitioner().partitionByDataSet(datasets, labels, propTrain)
    }
}