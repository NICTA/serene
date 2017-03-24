package au.csiro.data61.matcher.matcher.interface

import au.csiro.data61.matcher.ingestion.loader._
import au.csiro.data61.matcher.data._

import java.io._

case class LabelManualInterface(datasourcePath: String, dataRepoPath: String, classListPath: String) {

    val dataLoader = CsvDataLoader()
    val datasource = dataLoader.load(datasourcePath)
    val attributes = DataModel.getAllAttributes(datasource)

    def getCommandsHelp() = {
        """Commands:
          #
          #    h - Show this help screen for commands.
          #    s - Show all attributes.
          #    c - List classes.
          #    q - Discard changes and exit.
          #    x - Save and exit.
          #    nc <classname>  - Create new class.
          #    dc <classname>  - Delete class.
          #    l <attr_index> <class_name> - Label attribute.
        """.stripMargin('#')
    }

    def printAttributes() = {
        println(attributes.map({case Attribute(id, _, _, _) => id}).mkString("\n"))
    }

    def run() = {
        println(getCommandsHelp())
        val br = new BufferedReader(new InputStreamReader(System.in))
        readAndExecuteCommand(br, None)
    }

    def readAndExecuteCommand(reader: BufferedReader, pager: Option[Pager]): Unit = {
        println("Enter command: ")
        val nextCommand = reader.readLine()
        if(nextCommand.equalsIgnoreCase("q")) {
           //terminate 
        } else
        if(nextCommand.equalsIgnoreCase("s")) {
            val newpager = AttributesPager(attributes)
            println(newpager.getNext().map({case (idx, attr) => idx + ": " + attr.id + "[unknown]"}).mkString("\n"))
            if(newpager.hasNext) {
                println("    Type 'm' to show more.")
                readAndExecuteCommand(reader, Some(newpager))
            } else {
                readAndExecuteCommand(reader, None)
            }
        } else
        if(nextCommand.equalsIgnoreCase("m")) {
            pager match {
              case Some(p) => 
                  println(p.getNext().map({case (idx, attr) => idx + ": " + attr.id + "[unknown]"}).mkString("\n"))
                  if(p.hasNext) {
                      println("    Type 'm' to show more.")
                      readAndExecuteCommand(reader, Some(p))
                  } else {
                      readAndExecuteCommand(reader, None)
                  }
              case None => 
                  println("Nothing to show.")
                  readAndExecuteCommand(reader, None)
            }
        }
    }
}