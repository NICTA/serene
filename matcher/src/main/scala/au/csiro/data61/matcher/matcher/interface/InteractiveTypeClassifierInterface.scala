package au.csiro.data61.matcher.matcher.interface

import au.csiro.data61.matcher.ingestion.loader._
import au.csiro.data61.matcher.data._

import java.io._


case class InteractiveTypeClassifierInterface(datasourcePath: String, modelPath: String, labelsPath: String, classListPath: String) {

    val dataLoader = CsvDataLoader()
    val datasource = dataLoader.load(datasourcePath)
    val attributes = DataModel.getAllAttributes(datasource)

    def getCommandsHelp() = {
        """Commands:
          #
          #    h   - Show this help screen for commands.
          #    pd  - Show all predictions sorted most confident first.
          #    pi  - Show all predictions sorted least confident first.
          #    a <index> - Accept prediction.
          #    r <index> - Reject prediction.
          #    l <index> <class_name> - Label attribute.
          #    i - rerun inference.
          #    c - List classes.
          #    q - Discard changes and exit.
          #    x - Save and exit.
          #    nc <classname>  - Create new class.
          #    dc <classname>  - Delete class.
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
        if(nextCommand.equalsIgnoreCase("pd")) {
            println(
              """Predictions:
                #
                #0: agent@tableName - (NAME, confidence: 0.95)
                #1: dimension@family@tableName - (SIZE, confidence: 0.90)
                #2: agent_phone@tableName - (PHONE, confidence: 0.80)
                #3: fireplace@tableName - (BOOL_AMENITY, confidence: 0.80)
                #4: house_type@tableName - (NAME, confidence: 0.79)
                #5: level@bedroom_4@tableName - (LEVEL, confidence: 0.75)
                #6: dimension@recreation_room@tableName - (SIZE, confidence: 0.70)
                #7: dimension@bath_4@tableName - (SIZE, confidence: 0.60)
                #8: house_location@tableName - (ADDRESS, confidence: 0.59)
                #9: level@dining@tableName - (LEVEL, confidence: 0.55)
                #10: firm@tableName - (NAME, confidence: 0.54)
                #11: level@master@tableName - (LEVEL, confidence: 0.50)
                #12: dimension@bedroom_3@tableName - (SIZE, confidence: 0.45)
                #13: baths@tableName - (BOOL_AMENITY, confidence: 0.30)
                #14: level@kitchen@tableName - (LEVEL, confidence: 0.20)
                #15: level@bath_2@tableName - (LEVEL, confidence: 0.15)
                #16: school_dist@tableName - (ADDRESS, confidence: 0.05)
                #    Type 'm' to show more.
              """.stripMargin('#')
            )
            readAndExecuteCommand(reader, None)
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