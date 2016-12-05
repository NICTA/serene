package au.csiro.data61.matcher.nlptools.semantic

import edu.smu.tspell.wordnet._

object JAWSWordNetSynProvider {
    System.setProperty("wordnet.database.dir", "/usr/local/WordNet-3.0/dict");

    def findSynonyms(w: String): Set[String] = {
        val db = WordNetDatabase.getFileInstance()
        val synsets = db.getSynsets(w)
        synsets.flatMap(_.getWordForms).toSet
    }
}