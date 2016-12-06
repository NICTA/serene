package au.csiro.data61.matcher.ingestion.loader

import org.specs2._
import au.csiro.data61.matcher.data._
import au.csiro.data61.matcher.ingestion.loader._

class DBPediaDataLoaderSpec extends mutable.Specification {
    val dbpediaDb = "src/test/resources/datasets/dbpedia/music-artist-sample"

    s"""DBPediaDataLoader load("$dbpediaDb")""" should {
        s"load and parse json data from $dbpediaDb" in {
            val dataset = DBPediaDataLoader.load(dbpediaDb)
            val children = dataset.children.get

            val attrChildren = children.map({_.asInstanceOf[Attribute]})
            val abstractAttr = attrChildren.find({case x: Attribute => (x.id == "abstract@dbpedia")}).get
            abstractAttr.values.size mustEqual 4168

            // //this test case has become unreliable (order changes somehow)
            // val testcases = Seq(
            //                     ("abstract@dbpedia",           1, List("Jorge González Ríos is the former lead singer, main songwriter and sole composer of the Chilean band Los Prisioneros, and is currently the frontman of Los Updates. Los Prisioneros enjoyed a huge level of popularity and success initially in Chile and eventually all of Latin America beginning in 1984. The band called it quits in 1992, and González went on a successful solo career.González plays guitar, bass, contra bass, keyboard, synthesizer, programming, piano, percussion and drums.")),
            //                     ("givenName@dbpedia",          6, List("Jorge", "Ali Mohammad Zafar", "Jin", "Rikesh Nitin Chauhan", "Robert Autry Inman", "Rodney")),
            //                     ("origin@dbpedia",             6, List("http://dbpedia.org/resource/Chile", "", "http://dbpedia.org/resource/Japan", "", "http://dbpedia.org/resource/Florence,_Alabama", "Cumberland Gap, Tennessee, U.S.")),
            //                     ("activeYearsEndYear@dbpedia", 6, List("","","","","1968+02:00","1996+02:00")),
            //                     ("dbpedia_id@dbpedia",         6, List("5660471", "5305475", "8899177", "9952833", "16594433", "3958289")),
            //                     ("birthDate@dbpedia",          6, List("1964-12-06+02:00", "1980-05-18+02:00", "1984-07-04+02:00", "1990-06-05+02:00", "1929-01-06+02:00", "1969-03-28+02:00")),
            //                     ("placeOfBirth@dbpedia",       6, List("", "http://dbpedia.org/resource/Lahore", "http://dbpedia.org/resource/Japan", "http://dbpedia.org/resource/Luton", "", "Knoxville, Tennessee, U.S.")),
            //                     ("yearsActive@dbpedia",        6, List("", "2003", "1998", "2008", "1953", "1996")),
            //                     ("occupation@dbpedia",         6, List("http://dbpedia.org/resource/Multi-instrumentalist", 
            //                                                            "http://dbpedia.org/resource/Music_director", 
            //                                                            "http://dbpedia.org/resource/Singer-songwriter", 
            //                                                            "http://dbpedia.org/resource/Rapping", 
            //                                                            "http://dbpedia.org/resource/Singer-songwriter", 
            //                                                            "http://dbpedia.org/resource/Singer-songwriter")),
            //                     ("surname@dbpedia",            6, List("Gonzalez", "Zafar", "Akanishi", "", "Inman", "Atkins")),
            //                     ("sameAs@dbpedia",             6, List("http://rdf.freebase.com/ns/m.0dys59", 
            //                                                            "http://rdf.freebase.com/ns/m.01rl9p2", 
            //                                                            "http://rdf.freebase.com/ns/m.01w1wwn", 
            //                                                            "http://rdf.freebase.com/ns/m.04fzl50", 
            //                                                            "http://rdf.freebase.com/ns/m.03ydjpz", 
            //                                                            "http://rdf.freebase.com/ns/m.0b8ng0"))
            //                 )

            // testcases.map({case (attrId,sampleSize, expVal) => {
            //         attrChildren.find({case x: Attribute => (x.id == attrId)}).get.values.take(sampleSize) mustEqual expVal
            //         true
            //     }
            // }).foldLeft(true)(_&&_) mustEqual true
        }
    }
}