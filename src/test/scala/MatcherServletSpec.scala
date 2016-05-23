import org.scalatra.test.specs2.MutableScalatraSpec

class MatcherServletSpec extends MutableScalatraSpec {
  addServlet(classOf[MatcherServlet], "/*")

  "GET / on MatcherServlet" should {
    "return a greeting" in {
      get("/v1.0") {
        body must equalTo("""{"greeting":"Hello","to":"World"}""")
      }
    }
  }
}
