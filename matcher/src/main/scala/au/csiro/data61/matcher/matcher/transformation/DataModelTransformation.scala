package au.csiro.data61.matcher.matcher.transformation

import au.csiro.data61.matcher.data._

import sys.process._
import java.io._
import com.typesafe.scalalogging.LazyLogging

/**
 *  The source pattern and output format must conform to sed's
 *  syntax with extended regex e.g.:
 *      sed -E 's/([0-9]+)-([0-9]+)-([0-9]+)/\1\2\3/')
 *      where ([0-9]+)-([0-9]+)-([0-9]+) is an example sourcePattern
 *      and \1\2\3 is an example outputFormat.
 **/
case class Transformation(semanticClass: String, sourcePattern: String, outputFormat: String) extends LazyLogging {
    def applyTransformation(values: List[String]): List[String] = {
        val input = values.mkString("\n")
        val instream = new ByteArrayInputStream(input.getBytes("UTF-8"))
        val extendedRegexFlag = if(System.getProperty("os.name").toLowerCase.startsWith("mac")) {
            "-E"
        } else {
            "-r"
        }
        val command = s"sed $extendedRegexFlag s/${sourcePattern}/${outputFormat}/"
        try {
            logger.info("        running command: " + command)
            val result: String = (Process(command) #< instream).!!
            result.split("\n").toList
        } catch {
            case e: Exception =>
                logger.warn(s"        Failed to apply transformation for $semanticClass.  Reverting to raw data.")
                values
        }
    }
}

object DataModelTransformation extends LazyLogging {
    type Label = (String,String,Double,String,String,String)

    //TODO: this won't work for nested datamodels
    def apply(data: DataModel, transformations: List[Transformation], labels: SemanticTypeLabels): DataModel = {
        data.children match {
            case None => data
            case Some(children) => {
                lazy val updatedData = new DataModel(data.id, data.metadata, data.parent, Some(updatedChildren))

                //apply transformations to children
                lazy val updatedChildren = children.map({
                    case dm: DataModel => {
                        apply(dm, transformations, labels)
                    }
                    case attr @ Attribute(id,meta,values,parent) => {
                        val label = labels.findLabel(id)
                        transformations.find((t: Transformation) => {t.semanticClass == label}).map({case t =>
                            logger.info("    applying transformation for attribute " + id + " as " + label + " class.")
                            // println("        length of values is " + values.size)
                            new Attribute(id, meta, t.applyTransformation(values), parent)
                        }).getOrElse(attr)
                    }
                    case x => 
                        //TODO: this case shouldn't happen anyway.  maybe seal the trait(s) and remove this case?
                        x
                })
                updatedData
            }
        }
    }
}