package au.csiro.data61.matcher.matcher.eval

import org.specs2._
import au.csiro.data61.matcher.matcher.eval._
import au.csiro.data61.matcher.data._
import au.csiro.data61.matcher.ingestion.loader._

class DataPartitionerSpec extends mutable.Specification {
    val folder = "src/test/resources/datasets/wisc/realestate1"
    val descTemplate: (String => String) = (x: String) => s"WISC - $x Dataset"

    s"""DataPartitionerSpec partitionByAttribute() on WISC data""" should {
        "partition the WISC dataset by attribute only (disregarding source/container/datasource)" in {
            val datasetPaths = List((s"$folder/homeseekers", "homeseekers", descTemplate("Homeseekers")),
                (s"$folder/Texas", "texas", descTemplate("Texas")),
                (s"$folder/NKY", "nky", descTemplate("NKY")),
                (s"$folder/Windermere", "windermere", descTemplate("Windermere")),
                (s"$folder/yahoo", "yahoo", descTemplate("Yahoo!")))

            val datasets = datasetPaths.map({case (path,id,desc) => XmlDataLoader(id,id,desc).load(path)}).toList
            val labels = PositiveOnlyLabelsLoader().load(s"$folder/mappings-combined.csv")

            val partitionedDatasets = DataPartitioner().partitionByAttribute(datasets, labels, 0.7)
            (partitionedDatasets._1._1.flatMap(DataModel.getAllAttributes).toSet 
                & partitionedDatasets._2._1.flatMap(DataModel.getAllAttributes).toSet).size mustEqual 0
        }
    }

    s"""DataPartitionerSpec partitionByAttribute() on Freebase data""" should {
        "partition the WISC dataset by attribute only (disregarding source/container/datasource)" in {
            val dataDir = "src/test/resources/datasets/"
            val freebaseDb = FreebaseDataLoader.load(s"$dataDir/freebase/music-artist-sample")
            val dbpediaDb = DBPediaDataLoader.load(s"$dataDir/dbpedia/music-artist-sample")
            val labels = PositiveOnlyLabelsLoader().load(s"$dataDir/freebase-dbpedia-labels/music-artist/sample-dataset-labels/labels.txt")

            val partitionedDatasets = DataPartitioner().partitionByAttribute(List(freebaseDb,dbpediaDb), labels, 0.7)
            (partitionedDatasets._1._2 match {
                case l: ContainsPositiveLabels => l.positiveLabels.toList
                case _ => List()
            }).size mustEqual 4
            (partitionedDatasets._2._2 match {
                case l: ContainsPositiveLabels => l.positiveLabels.toList
                case _ => List()
            }).size mustEqual 3
            (partitionedDatasets._1._1.flatMap(DataModel.getAllAttributes).toSet 
                & partitionedDatasets._2._1.flatMap(DataModel.getAllAttributes).toSet).size mustEqual 0
        }
    }    

    s"""DataPartitionerSpec partitionByDataSet() on WISC data""" should {
        "partition the WISC dataset by data source" in {
            val datasetPaths = List((s"$folder/homeseekers", "homeseekers", descTemplate("Homeseekers")),
                (s"$folder/Texas", "texas", descTemplate("Texas")),
                (s"$folder/NKY", "nky", descTemplate("NKY")),
                (s"$folder/Windermere", "windermere", descTemplate("Windermere")),
                (s"$folder/yahoo", "yahoo", descTemplate("Yahoo!")))

            val datasets = datasetPaths.map({case (path,id,desc) => XmlDataLoader(id,id,desc).load(path)}).toList
            val labels = PositiveOnlyLabelsLoader().load(s"$folder/mappings-combined.csv")

            val partitionedDatasets = DataPartitioner().partitionByDataSet(datasets, labels, 0.7)

            (partitionedDatasets._1._1.flatMap(DataModel.getAllAttributes).toSet 
                & partitionedDatasets._2._1.flatMap(DataModel.getAllAttributes).toSet).size mustEqual 0

            (partitionedDatasets._1._1.flatMap(DataModel.getAllAttributes).map(_.id).toSet 
                & partitionedDatasets._2._1.flatMap(DataModel.getAllAttributes).map(_.id).toSet).size mustEqual 0

            ((partitionedDatasets._1._2 match {
                case l: ContainsPositiveLabels => l.positiveLabels.toList
                case _ => List()
            }).flatMap(_.toList).toSet
                & (partitionedDatasets._2._2 match {
                      case l: ContainsPositiveLabels => l.positiveLabels.toList
                      case _ => List()
                  }).flatMap(_.toList).toSet).size mustEqual 0
        }
    }   
}