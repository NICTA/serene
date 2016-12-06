package au.csiro.data61.matcher.nlptools.semantic

import org.specs2._
import au.csiro.data61.matcher.nlptools.semantic._

class WordNetSynProviderSpec extends mutable.Specification {
	"""WordNetSynProviderSpec findSynonyms("address") """ should {
        """contain "destination" """ in {
        	val synset1 = WordNetSynProvider().findSynonyms("address")
        	synset1 must contain("residence")
        }
    }

    """WordNetSynProviderSpec findSynonyms("residence") """ should {
        """must contain address""" in {
        	val synset1 = WordNetSynProvider().findSynonyms("residence")
        	synset1 must contain("address")
        }
    }

    """WordNetSynProviderSpec findSynonyms("staff") """ should {
        """must contain personnel""" in {
            val synset1 = WordNetSynProvider().findSynonyms("staff")
            synset1 must contain("personnel")
        }
    }

    // //Why are these not linked in WordNet?
    // """WordNetSynProviderSpec findSynonyms("sex") """ should {
    //     """must contain gender""" in {
    //         val synset1 = WordNetSynProvider().findSynonyms("sex")
    //         synset1 must contain("gender")
    //     }
    // }
}