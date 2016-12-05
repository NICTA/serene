package au.csiro.data61.matcher.ingestion.loader

import java.nio.file.Paths

import au.csiro.data61.matcher.data._
import au.csiro.data61.matcher.matcher._
import au.csiro.data61.matcher.matcher.eval.datasetutils._
import au.csiro.data61.matcher.matcher.eval.metrics._
import au.csiro.data61.matcher.ingestion.loader._
import au.csiro.data61.matcher.nlptools.distance.LCSubsequenceDistanceMetric

import au.csiro.data61.matcher.matcher.train._

import java.io._

case class CSVDataWriter() {
    def writeDataModel(datasets: List[AbstractDataModel],
                       outputDirPath: String,
                       parentId: String = ""
                      ): Unit = datasets.head match {
        case table: DataModel =>
            datasets
              .asInstanceOf[List[DataModel]]
              .map { case table =>
                  writeDataModel(table.children.get, outputDirPath, table.metadata.get.name)
              }

        case attribute: Attribute =>
            val attributes = datasets.asInstanceOf[List[Attribute]]

            //build headers
            val headers = attributes.map({x => truncateSuffix(x.id)}).mkString(",")

            //build content
            val attrVals = attributes.map(_.values)
            val attrValsHead = attrVals
              .head
              .map { case v =>
                  List("\"" + cleanString(v) + "\"")
              }
            val attrValsCsv = attrVals
              .tail
              .foldLeft(attrValsHead)((head: List[List[String]], attrvals: List[String]) => {
                (head zip attrvals).map({case (l: List[String], e: String) => l :+ ("\"" + cleanString(e) + "\"")})
            })

            val absFilePath = Paths.get(outputDirPath, parentId).toString
            val out = new PrintWriter(new File(absFilePath))
            out.println(headers)
            out.println(attrValsCsv.map(_.mkString(",")).mkString("\n"))
            out.close()
    }

    def truncateSuffix(attrId: String): String = {
        val nameRegex1 = "^(.+)@([^@]+)@([^@]+)$".r
        val nameRegex2 = "^([^@]+)@([^@]+)$".r
        attrId match {
            case nameRegex1(name,table,db) => name
            case nameRegex2(name,db) => name
        }
    }

    def cleanString(s: String): String = {
        s.replaceAll("\n", " ").replaceAll("\"", "'")
    }
}