package au.csiro.data61.matcher.matcher.eval.datasetutils

import au.csiro.data61.matcher.data._
import au.csiro.data61.matcher.ingestion.loader._
import au.csiro.data61.matcher.matcher.eval._

import scala.util._

trait NorthixUtils {
	def loadDataSets(): List[DataModel] = {
        val db1Path = "src/test/resources/datasets/northix/db1"
        val db2Path = "src/test/resources/datasets/northix/db2"
        val northixDS1 = NorthixDataLoader("1.dat").load(db1Path)
        val northixDS2 = NorthixDataLoader("2.dat").load(db2Path)
        List(northixDS1,northixDS2)
    }

    def loadLabels(): Labels = {
        val gtPath = "src/test/resources/datasets/northix/gt"
        NorthixDataLoader("Northix").loadLabels(gtPath)
    }

    def partitionDataSet(datasets: List[DataModel], labels: Labels, propTrain: Double, randNumGenerator: Random = new Random(1000)): ((List[DataModel],Labels), (List[DataModel],Labels)) = {
        DataPartitioner().partitionByAttribute(datasets, labels, propTrain)
    }
}