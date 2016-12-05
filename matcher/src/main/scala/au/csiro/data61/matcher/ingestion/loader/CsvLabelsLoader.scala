package com.nicta.dataint.ingestion.loader

import scala.io.Source

case class CsvLabelsLoader() {
    def loadLabels(path: String): Seq[Set[String]] = {
        Source
          .fromFile(path)
          .getLines
          .filter { case l => !(l.startsWith("#") || l.trim.length == 0)}
          .map(_.split(",").toSet).toSeq
    }
}