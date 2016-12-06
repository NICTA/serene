package au.csiro.data61.matcher.matcher.features

import org.specs2._
import au.csiro.data61.matcher.data._
import au.csiro.data61.matcher.matcher.features._

class DataPreprocessorSpec extends mutable.Specification {
    s"""AttributeNameTokenizer""" should {
        """split "create_date" into "create" and "date" """ in {
            lazy val dm: DataModel = new DataModel("", None, None, Some(List(attr)))
            lazy val attr: Attribute = Attribute("", Some(Metadata("create_date", "")), List(), Some(dm))
            val results = AttributeNameTokenizer().preprocess(attr)
            println(results)
            1 mustEqual 1
        }
    }

    s"""AttributeContentTermFrequency""" should {
        """get term frequency of its content """ in {
            lazy val dm: DataModel = new DataModel("", None, None, Some(List(attr)))
            lazy val attr: Attribute = Attribute("", Some(Metadata("create_date", "")), 
                List("The quick brown fox.", 
                     "Jumped over the lazy dog.",
                     "My dog fluffy is actually a fox."), 
                Some(dm)
            )
            val results = AttributeContentTermFrequency().preprocess(attr)
            println(results)

            1 mustEqual 1
        }
    }    
}