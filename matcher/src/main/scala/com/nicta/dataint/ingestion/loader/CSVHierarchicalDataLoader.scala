package com.nicta.dataint.ingestion.loader

import java.io.File
import java.nio.file.Paths

import scala.io.Source

import com.nicta.dataint.data.Metadata
import com.nicta.dataint.data.DataModel
import com.nicta.dataint.data.Attribute

case class CSVHierarchicalDataLoader(val encoding: String = "utf-8") {

    def readDataSets(rootDataSetPath: String, relFilePath: String): List[DataModel] = {
        val absFilePath = Paths.get(rootDataSetPath, relFilePath).toString
        val f = new File(absFilePath)

        if(f.isDirectory) {
            f.listFiles.filter({!_.getName.startsWith(".")}).toList.flatMap({case cf =>
                val childPath = cf.getAbsolutePath
                val pathRegex = s"$rootDataSetPath/(.+)".r
                childPath match {
                    case pathRegex(relPath) => readDataSets(rootDataSetPath, relPath)
                }
            })
        } else {
            List(readDataModel(f, relFilePath))
        }
    }

    def readDataSet(rootDataSetPath: String, relFilePath: String): DataModel = {
        val absFilePath = Paths.get(rootDataSetPath, relFilePath).toString
        readDataModel(new File(absFilePath), relFilePath)
    }

    def readDataModel(file: File, dataSetId: String): DataModel = {
        val source = Source.fromFile(file, encoding)
        val lines = source.getLines.toList
        source.close

        val quotedRegex = "\"(.*)\"" .r
        val attrHeaders = lines.head.split(""",(?=(?:[^\"]*\"[^\"]*\")*(?![^\"]*\"))""", -1).toList.map({
            case quotedRegex(a) => a.trim()
            case x => x.trim()
        })
        val attrVals = lines.drop(1).map({case line => line.split(""",(?=(?:[^\"]*\"[^\"]*\")*(?![^\"]*\"))""", -1).toList})

        lazy val table: DataModel = new DataModel(dataSetId, Some(Metadata(dataSetId,"")), None, Some(attributes))
        lazy val attributes: List[Attribute] = (0 until attrHeaders.size).map({case idx => {
                new Attribute(s"$dataSetId/${attrHeaders(idx)}",
                              Some(Metadata(attrHeaders(idx),"")),
                              attrVals.map({
                                case tokens if tokens.size > idx =>
                                    val t = tokens(idx)
                                    if(t.length > 1 && t.startsWith("\"") && t.endsWith("\"")) t.substring(1,t.length-1) //remove quotes
                                    else t
                                case _ => ""
                              }).toList,
                              Some(table)
                              )
            }
        }).toList

        table
    }
}
