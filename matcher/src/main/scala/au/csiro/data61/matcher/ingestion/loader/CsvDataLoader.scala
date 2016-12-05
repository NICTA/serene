package au.csiro.data61.matcher.ingestion.loader

import java.io.File

import scala.io.Source

import au.csiro.data61.matcher.data.Metadata
import au.csiro.data61.matcher.data.DataModel
import au.csiro.data61.matcher.data.Attribute
import au.csiro.data61.matcher.data._

case class CSVDataLoader(val id: String = "", val encoding: String = "utf-8") extends FileLoaderTrait[DataModel] {

    def load(path: String): DataModel = {
        //path can point to either a directory or a file
        if((new File(path)).isDirectory()) {
            loadDirectory(path)
        } else {
            loadFile(path)
        }
    }

    def loadDirectory(path: String): DataModel = {
        val tableNameRegex = """^(.+).csv""".r
        val tableNames = new File(path).list.filter({
            case tableNameRegex(a) => true
            case _ => false
        }).map({
            case tableNameRegex(a) => a + ".csv"
        })

        lazy val csvData: DataModel = new DataModel(id, Some(Metadata("CSV Dataset", "CSV Dataset")), None, Some(tables))
        lazy val tables: List[DataModel] = tableNames.map(loadTable(path,_,id,Some(csvData))).toList

        csvData
    }

    def loadFile(path: String): DataModel = {
        val source = Source.fromFile(path, encoding)
        val lines = source.getLines.toList.toSeq
        val filename = path.substring(path.lastIndexOf("/")+1, path.length)
        source.close
        parseCsv(lines, filename, "", None)
    }

    def loadTable(path: String, tableName: String, parentId: String): DataModel = {
        loadTable(path, tableName, parentId, None)
    }

    def loadTable(path: String, tableName: String, parentId: String, parent: => Option[DataModel]): DataModel = {
        val source = Source.fromFile(s"$path/$tableName", encoding)
        val lines = source.getLines.toList.toSeq
        source.close
        parseCsv(lines, tableName, parentId, parent)
    }

    def parseCsv(lines: Seq[String], tableName: String, parentId: String, parent: => Option[DataModel]): DataModel = {
        val quotedRegex = "\"(.*)\"" .r
        val attrHeaders = lines.head.split(""",(?=(?:[^\"]*\"[^\"]*\")*(?![^\"]*\"))""", -1).toList.map({
            case quotedRegex(a) => a.trim()
            case x => x.trim()
        })
        val attrVals = lines.drop(1).map({case line => line.split(""",(?=(?:[^\"]*\"[^\"]*\")*(?![^\"]*\"))""", -1).toList})

        lazy val table: DataModel = new DataModel(tableName, Some(Metadata(tableName,"")), parent, Some(attributes))
        lazy val attributes: List[Attribute] = (0 until attrHeaders.size).map({case idx => {
                new Attribute(if(parentId.length > 0) s"${attrHeaders(idx)}@$tableName@$parentId" else s"${attrHeaders(idx)}@$tableName", 
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