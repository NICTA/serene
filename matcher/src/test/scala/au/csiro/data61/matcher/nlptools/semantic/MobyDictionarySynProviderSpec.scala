package au.csiro.data61.matcher.nlptools.semantic

import org.specs2._

class MobyDictionarySynProviderSpec extends mutable.Specification {
  val synProvider = MobyDictionarySynProvider()

  """MobyDictionarySynProviderSpec findSynonyms("address") """ should {
        """contain "residence" """ in {
          val synset1 = synProvider.findSynonyms("address")
          synset1 must contain("residence")
          synset1 must contain("destination")
          synset1 must contain("delivery")
        }
    }

    """MobyDictionarySynProviderSpec findSynonyms("sex") """ should {
        """must contain gender""" in {
          val synset1 = synProvider.findSynonyms("sex")
          synset1 must contain("gender")
        }
    }

    """MobyDictionarySynProviderSpec findSynonyms("staff") """ should {
        """must contain personnel""" in {
            val synset1 = synProvider.findSynonyms("staff")
            synset1 must contain("personnel")
        }
    }

    """MobyDictionarySynProviderSpec findSynonyms("customer") """ should {
        """must not contain phone, car, table""" in {
            val synset1 = synProvider.findSynonyms("customer")
            synset1 must not contain("phone")
            synset1 must not contain("car")
            synset1 must not contain("table")
        }
    }    

    """MobyDictionarySynProviderSpec findSynonyms("customer") """ should {
        """must contain client""" in {
            val synset1 = synProvider.findSynonyms("customer")
            synset1 must contain("client")
        }
    }

    """MobyDictionarySynProviderSpec findSynonyms("age") """ should {
        """must not contain phone""" in {
            val synset1 = synProvider.findSynonyms("age")
            synset1 must not contain("phone")
        }
    }            
}