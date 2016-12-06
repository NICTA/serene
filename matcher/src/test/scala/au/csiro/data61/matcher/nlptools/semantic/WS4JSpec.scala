package au.csiro.data61.matcher.nlptools.semantic

import org.specs2._
import edu.cmu.lti.ws4j._

import edu.cmu.lti.jawjaw.JAWJAW;
import edu.cmu.lti.lexical_db.ILexicalDatabase;
import edu.cmu.lti.lexical_db.NictWordNet;
import edu.cmu.lti.ws4j.impl.HirstStOnge;
import edu.cmu.lti.ws4j.impl.JiangConrath;
import edu.cmu.lti.ws4j.impl.LeacockChodorow;
import edu.cmu.lti.ws4j.impl.Lesk;
import edu.cmu.lti.ws4j.impl.Lin;
import edu.cmu.lti.ws4j.impl.Path;
import edu.cmu.lti.ws4j.impl.Resnik;
import edu.cmu.lti.ws4j.impl.WuPalmer;
import edu.cmu.lti.ws4j.util.MatrixCalculator;

import edu.cmu.lti.lexical_db.data.Concept;
import edu.cmu.lti.jawjaw.util.WordNetUtil;
import edu.cmu.lti.jawjaw.pobj.POS;

import java.util._

class WS4JSpec extends mutable.Specification {

  """WS4JSpec compare "address" and "residence" """ should {
        """show strong connection""" in {
              val word1 = "residence#n"
              val word2 = "address#n"

              println("WUP:" + WS4J.runWUP(word1, word2))
              println("JCN:" + WS4J.runJCN(word1, word2))

              println("amount and order: " + WS4J.runWUP("amount","order"))
              println("amount and date: " + WS4J.runWUP("amount","date"))

              1 mustEqual 1
        }
    }

    def toSynsets(word: String, posText: String = "n"): ArrayList[Concept] = {
        val pos2 = POS.valueOf(posText); 
        val synsets = WordNetUtil.wordToSynsets(word, pos2);
        val concepts = new ArrayList[Concept](synsets.size());

        for(i <- 0 until synsets.size) {
            val synset = synsets.get(i)
            concepts.add( new Concept(synset.getSynset(), POS.valueOf(posText)) );
        }
        return concepts;
    }    
}