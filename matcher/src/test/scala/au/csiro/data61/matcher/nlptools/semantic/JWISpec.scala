package au.csiro.data61.matcher.nlptools.semantic

import org.specs2._
import au.csiro.data61.matcher.nlptools.semantic._

import edu.mit.jwi._
import edu.mit.jwi.data._
import edu.mit.jwi.data.compare._
import edu.mit.jwi.data.parse._
import edu.mit.jwi.item._
import edu.mit.jwi.morph._


// class JWISpec extends mutable.Specification {
// 	//This is only meant to demonstrate using the JWI library
// 	"""JWISpec findSynonyms("address") """ should {
//         """contain "destination" """ in {
//         	// construct the URL to the Wordnet dictionary directory
// 		    val dict: IDictionary = new Dictionary(new java.net.URL("file:///usr/local/WordNet-3.0/dict"))
// 		    dict.open();
		
// 		    val idxWord: IIndexWord = dict.getIndexWord("staff", POS.NOUN);
//             val wordIds = idxWord.getWordIDs()

//             for(i <- 0 until wordIds.size) {
//             	println("----------------")
//             	val wordID: IWordID = idxWord.getWordIDs().get(i);
// 		        val word: IWord = dict.getWord(wordID);
//                 println("Id = " + wordID);
// 		        println("Lemma = " + word.getLemma());
// 		        println("Gloss = " + word.getSynset().getGloss());

//                 val relSynsetIds = word.getSynset.getRelatedSynsets
//                 for(j <- 0 until relSynsetIds.size) {
//                 	val synsetId: ISynsetID = relSynsetIds.get(j)
//                 	val relSynset: ISynset = dict.getSynset(synsetId)

//                     println("    Id = " + synsetId);
// 		            println("    RelSynset = " + relSynset);
// 		            println("    RelSynsetGloss = " + relSynset.getGloss)
//                 }
		        
//             }

//         	1 mustEqual 1
//         }
//     }
// }