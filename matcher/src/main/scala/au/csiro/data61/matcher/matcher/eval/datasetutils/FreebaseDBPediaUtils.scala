package au.csiro.data61.matcher.matcher.eval.datasetutils

import au.csiro.data61.matcher.data._
import au.csiro.data61.matcher.ingestion.loader._
import au.csiro.data61.matcher.matcher.eval._

import scala.util._

trait FreebaseDBPediaUtils {
    def loadDataSets(): List[DataModel] = {
        val db1Path = "src/test/resources/datasets/freebase/music-artist-sample"
        val db2Path = "src/test/resources/datasets/dbpedia/music-artist-sample"
        val freebaseDb = FreebaseDataLoader.load(db1Path)
        val dbpediaDb = DBPediaDataLoader.load(db2Path)
        List(freebaseDb,dbpediaDb)
    }

    def loadLabels(): Labels = {
        val gtPath = "src/test/resources/datasets/freebase-dbpedia-labels/music-artist/sample-dataset-labels/labels.txt"
        PositiveOnlyLabelsLoader().load(gtPath)
    }

    def partitionDataSet(datasets: List[DataModel], labels: Labels, propTrain: Double, randNumGenerator: Random = new Random(1000)): ((List[DataModel],Labels), (List[DataModel],Labels)) = {
        DataPartitioner().partitionByAttribute(datasets, labels, propTrain)
    }
}