package au.csiro.data61.matcher.nlptools.semantic

import edu.mit.jwi._
import edu.mit.jwi.item._
import edu.mit.jwi.morph._
import edu.mit.jwi.data._
import edu.mit.jwi.data.compare._
import edu.mit.jwi.data.parse._

import scala.collection.JavaConverters._

case class WordNetSynProvider() {
    val dict: IDictionary = new Dictionary(new java.io.File(getClass.getClassLoader.getResource("wordnet/dict").getPath))
    dict.open();

    def findSynonyms(w: String): Set[String] = {
        val idxWord: IIndexWord = dict.getIndexWord(w, POS.NOUN);
        if(idxWord == null) return Set()
        else {
            val wordIds = idxWord.getWordIDs()
            if(wordIds == null) return Set()
            else {
                wordIds.asScala.flatMap({
                    case wordId => {
                        val word: IWord = dict.getWord(wordId);
                        word.getSynset.getRelatedSynsets.asScala.flatMap({
                            case synsetId => {
                                val relSynset: ISynset = dict.getSynset(synsetId) //TODO: WHY IS THIS NOT THREAD SAFE!
                                relSynset.getWords.asScala.map(_.getLemma)
                            }
                        })
                    }
                }).toSet + w
            }
        }
    }
}