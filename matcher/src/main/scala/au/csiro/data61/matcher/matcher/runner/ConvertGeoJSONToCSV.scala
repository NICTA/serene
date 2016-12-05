package au.csiro.data61.matcher.matcher.runner

import java.io._
import scala.io._

import org.json4s._
import org.json4s.native.JsonMethods._

/**
 *  Used to convert geojson data (e.g. citycouncil) into csv.
 **/

object ConvertGeoJSONToCSV {

    implicit lazy val formats = org.json4s.DefaultFormats

    def main(args: Array[String]): Unit = {
        val path = args(0)
        val outputPath = args(1)

        val outputDir = new File(outputPath)
        if(!outputDir.exists) {
            outputDir.mkdir
        }

        convertFiles(path, outputPath)
    }

    def convertFiles(inputPath: String, outputPath: String): List[Product] = {
        val files = (new File(inputPath)).list
        files.filter({f => !(f.equals(".") || f.equals(".."))}).map({f =>
            println("processing " + f)
            val file = new File(s"$inputPath/$f")
            // if(file.isDirectory) {
            //     convertFiles(s"$path/$f")
            // } else 
            if(f.endsWith(".json")) {
                val json = parse(Source.fromFile(file).getLines.foldLeft("")(_+"\n"+_))
                val records = (json \ "features").extract[JArray].extract[List[JObject]].map({record => 
                    ((record \ "properties") match {
                        case JObject(fields) => fields
                    }) map {
                        case (s, JString(v)) => (s,v)
                        case (s, JInt(v)) => (s,v)
                        case (s, JDouble(v)) => (s,v)
                        case (s, JArray(v)) => (s,v)
                        case (s, JNull) => (s,"null")
                    }
                })

                val attributes = records.flatMap({_.map({case (k,v) => k})}).distinct
                
                val out = new PrintWriter(new File(outputPath + "/" + f.substring(f.lastIndexOf("/")+1,f.lastIndexOf(".")) + ".csv"))

                out.println(attributes.mkString(","))
                val body = records.map({r =>
                    attributes.map({a =>
                        val d = r.find((keyValPair) => keyValPair match {case (k,v) => k == a}).map({case (k,v) => v}).getOrElse("null")
                        "\"" + d.toString.replaceAll("\"","") + "\""
                    }).mkString(",")
                }).mkString("\n")
                out.println(body)
                out.close()

                records
            } else {
                None
            }
        }).toList
    }
}