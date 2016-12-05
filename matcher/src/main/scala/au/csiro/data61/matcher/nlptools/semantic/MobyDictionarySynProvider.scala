package au.csiro.data61.matcher.nlptools.semantic

import scala.io._
import scala.collection.JavaConverters._
import java.io.File

case class MobyDictionarySynProvider() {
    val thesaurus: List[Set[String]] = Source.fromFile(getClass.getClassLoader.getResource("dict/moby-dictionary/mthesaur.txt").toURI).getLines.map({
        _.split(",").toSet
    }).toList

    def findSynonyms(w: String): Set[String] = {
        thesaurus.filter({_.contains(w)}).flatMap({case x => x}).toSet
    }
}