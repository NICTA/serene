package au.csiro.data61.matcher.nlptools.semantic

import org.specs2._
import au.csiro.data61.matcher.nlptools.semantic._

import net.didion.jwnl._
import net.didion.jwnl.data._
import net.didion.jwnl.data.relationship._
import net.didion.jwnl.dictionary.Dictionary
import net.didion.jwnl.data.list.PointerTargetNode

import scala.io._
import java.io._

// class JWNLSpec extends mutable.Specification {
// 	//This is only meant to demonstrate the JWNL library
// 	"""JWNLSpec findSynonyms("address") """ should {
//         """contain "destination" """ in {
//         	JWNL.initialize(new FileInputStream(new File("/home/privera/dropbox-personal/Dropbox/phd/data-integration-project/source-code/version-controlled/data-integration2/src/test/resources/file_properties.xml")))
//         	val dict = Dictionary.getInstance
//         	println("-------------1---------------")
//             val indexWordsSet = dict.lookupAllIndexWords("staff")
//             println("indexWords: " + indexWordsSet)
//             val indexWords = indexWordsSet.getIndexWordArray

//             val syns = indexWords.flatMap({
//             	case x => x.getSenses.map({
//             		case x: Synset => PointerUtils.getInstance().getSynonyms(x)
//                 })
//             })
//             val words = syns.flatMap({
//             	case x => x.toArray.flatMap({
//             		case x: PointerTargetNode => x.getSynset.getWords.map({case x=>x.getLemma})
//             	})
//             })
//             println("---------WORDS----------")
//             println(words.foldLeft("")(_+_))
//             println(words)

//         	1 mustEqual 1
//         }
//     }
// }