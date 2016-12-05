package au.csiro.data61.matcher.ingestion.loader

import java.io.File

import scala.io.Source

import au.csiro.data61.matcher.data.Metadata
import au.csiro.data61.matcher.data.DataModel
import au.csiro.data61.matcher.data.Attribute
import au.csiro.data61.matcher.data._

case class NorthixDataLoader(val id: String) extends FileLoaderTrait[DataModel] {
  
    def load(path: String): DataModel = {
        val suffixRegex = """^(.+)@(.+)@(.+)""".r
        val dbnames = new File(path).list.map({case suffixRegex(a,t,d) => d}).distinct
        
        lazy val northixData: DataModel = new DataModel(id,
                                                        Some(Metadata("Northix Dataset","Northix Dataset")),
                                                        None,
                                                        Some(dbs))
        lazy val dbs: List[DataModel] = dbnames.map(loadDb(path,_,northixData)).toList

        northixData
    }

    def loadDb(path: String, fnameSuffix: String, parent: => DataModel): DataModel = {
        val fnameRegex = """^(.+)@(.+)@(.+)""".r
        val files = new File(path).listFiles.filter(_.getName.endsWith(fnameSuffix))
        val filenames = files.map(_.getName)
        val parsedFilenames = filenames.map({
            case fnameRegex(attrname,tablename,dbname) => (attrname,tablename,dbname)
        })

        val tablenames = parsedFilenames.filter(_._3 == fnameSuffix).map(_._2).distinct

        lazy val db = new DataModel(fnameSuffix, Some(Metadata(fnameSuffix,s"Database $fnameSuffix")), Some(parent), Some(tables))
        lazy val tables: List[DataModel] = tablenames.map({case tablename => {
                lazy val t: DataModel = new DataModel(s"$tablename@$fnameSuffix", Some(Metadata(tablename,s"Table $tablename")), Some(parent), Some(attributes))
                lazy val attributes: List[Attribute] = parsedFilenames.filter(_._2 == tablename).map({
                    case (attrN,tblN,dbN) => {
                        lazy val attr: Attribute = new Attribute(s"$attrN@$tablename@$fnameSuffix", Some(Metadata(attrN, s"attribute $attrN")), Source.fromFile(s"$path/$attrN@$tblN@$dbN", "ISO-8859-1").getLines.toList, Some(t))
                        attr
                    }
                }).toList
                t
            }
        }).toList

        db
    }

    def loadLabels(path: String): Labels = {
        val classes = new File(path).list
        BasicLabels(classes.map(x => new File(path+"/"+x).list.toSet).toList)
    }
}