package au.csiro.data61.matcher.matcher.eval.datasetutils

import au.csiro.data61.matcher.data._
import au.csiro.data61.matcher.ingestion.loader._
import au.csiro.data61.matcher.matcher.eval._

import scala.util._

trait SimpleTestDBUtils {
  def loadDataSets(): List[DataModel] = {
        val ds1 = CsvDataLoader("source").load("src/test/resources/datasets/simpleCsvData/db1")
        val ds2 = CsvDataLoader("target").load("src/test/resources/datasets/simpleCsvData/db2")
        List(ds1,ds2)
    }

    def loadLabels(): Labels = {
        val gtPath = "src/test/resources/datasets/simpleCsvData/gt.csv"
        BasicLabels(CsvLabelsLoader().loadLabels(gtPath).toList)
    }

    def partitionDataSet(datasets: List[DataModel], labels: Labels, propTrain: Double, randNumGenerator: Random = new Random(1000)): ((List[DataModel],Labels), (List[DataModel],Labels)) = {
        DataPartitioner().partitionByAttribute(datasets, labels, propTrain)
    }
}