package com.nicta.dataint.ingestion.loader

import org.specs2._
import com.nicta.dataint.data._
import com.nicta.dataint.ingestion.loader._

class CSVDataLoaderSpec extends mutable.Specification {
    val sampleData = "src/test/resources/datasets/simpleCsvData/db1"

    // s"""CSVDataLoader load()""" should {
    //     s"load and parse csv data" in {
    //         val dataset = CSVDataLoader("testdb").load(sampleData)
    //         // val children = dataset.children.get
    //         println(dataset)

    //         1 mustEqual 1
    //     }
    // }

    s"""CSVDataLoader load centrelink-locations.csv""" should {
        s"load and parse csv data" in {
            val dataset = CSVDataLoader("testdb").load("src/test/resources/datasets/data.gov.au/db5")
            for(a <- DataModel.getAllAttributes(dataset)) {
                println("==========" + a.metadata.get.name + ":\n" + a.values)
            }
            1 mustEqual 1
        }
    }
}