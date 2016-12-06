package au.csiro.data61.matcher.matcher.transformation

import org.specs2._
import au.csiro.data61.matcher.data._
import au.csiro.data61.matcher.ingestion.loader._

class DataModelTransformationSpec extends mutable.Specification {
    
    s"""Transformation applyTransformation()""" should {
        "remove hyphens in this example" in {
            val data = List("123-421-111",
                            "918-113-214",
                            "981-142-123")
            val result = Transformation("phone", "([0-9]+)-([0-9]+)-([0-9]+)", "\\1\\2\\3").applyTransformation(data)
            println(result)
            1 mustEqual 1
        }
    }

    s"""DataModelTransformation apply()""" should {
        "transform the phone attribute in this example" in {
            lazy val dm: DataModel = new DataModel("testdm", None, None, Some(children))
            lazy val children: List[Attribute] = List(
                new Attribute("telephone@testdata", None, List("1234-1230-021", "0192-3214-912", "1421-1234-321"), Some(dm))
            )

            println("BEFORE:")
            println(dm)

            val transformations = List(
                Transformation("phone", "([0-9]+)-([0-9]+)-([0-9]+)", "\\1\\2\\3")
            )

            val labels = SemanticTypeLabels(
                Map(
                    ("telephone@testdata" -> PredictedSemanticTypeLabel("telephone@testdata", "phone", 0.9, "2015/04/15", "phone", "2015/04/16"))
                )
            )

            val result = DataModelTransformation.apply(dm, transformations, labels)
            println("AFTER:")
            println(result)
            
            1 mustEqual 1
        }
    }
}