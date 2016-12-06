package au.csiro.data61.matcher.ingestion.loader

import org.specs2._
import au.csiro.data61.matcher.data._
import au.csiro.data61.matcher.ingestion.loader._

class FreebaseDataLoaderSpec extends mutable.Specification {
    val freebaseDb = "src/test/resources/datasets/freebase/music-artist-sample"

    // //this test case has become unreliable (order changes somehow)
    // s"""FreebaseDataLoader load("$freebaseDb")""" should {
    //     s"load and parse json data from $freebaseDb" in {
    //         val dataset = FreebaseDataLoader.load(freebaseDb)
    //         val children = dataset.children.get

    //         val attrChildren = children.map({_.asInstanceOf[Attribute]})
    //         val nameAttr = attrChildren.find({case x: Attribute => (x.id == "name@freebase")}).get
    //         nameAttr.values.size mustEqual 700
    //         nameAttr.values.take(6) mustEqual List("Grandmaster Flash and the Furious Five", 
    //                                                "Shota Yasuda", "Mai Satoda", "Hiroko Moriguchi", 
    //                                                "Eddie Harsch", "Benjamin Wynn")

    //         val typeAttr = attrChildren.find({case x: Attribute => (x.id == "type@freebase")}).get
    //         typeAttr.values.size mustEqual 700
    //         typeAttr.values.take(6) must contain("/music/artist")

    //         val originAttr = attrChildren.find({case x: Attribute => (x.id == "origin@freebase")}).get
    //         originAttr.values.size mustEqual 700
    //         originAttr.values.take(6) mustEqual List("New York City", "Amagasaki", "Sapporo", 
    //                                                  "Fukuoka", "Detroit", "Chicago")

    //         val midAttr = attrChildren.find({case x: Attribute => (x.id == "freebase_mid@freebase")}).get
    //         midAttr.values.size mustEqual 700
    //         midAttr.values.take(6) mustEqual List("/m/01w58k9,/m/01lfnll,/m/034s02,/m/02rnzhp,/m/076y52r,/m/0q57hvp,/m/01v0f6h,/m/0_q_16x", 
    //             "/m/01w5j5t,/m/02qhvkc,/m/010xc505", "/m/01w5mfk,/m/0bzn3w", "/m/01w6pqz,/m/05q7zq1,/m/0jwd__v", 
    //             "/m/01w8n56,/m/047n1xt", "/m/01w8qr6,/m/01p7dty,/m/0jk_ymw")

    //         val idAttr = attrChildren.find({case x: Attribute => (x.id == "freebase_id@freebase")}).get
    //         idAttr.values.size mustEqual 700
    //         idAttr.values.take(6) mustEqual List("/en/grandmaster_flash", "/en/shota_yasuda", 
    //                                              "/en/mai_satoda", "/en/hiroko_moriguchi", 
    //                                              "/en/eddie_harsch", "/en/deru")
    //     }
    // }

    val recId1 = "/en/brian_eno"
    s"""FreebaseDataLoader load("$freebaseDb")""" should {
        s"have 'null' encoded as null for record with id $recId1 for age and height_meters" in {
            val dataset = FreebaseDataLoader.load(freebaseDb)
            val children = dataset.children.get
            val rec1Idx = children.find({case Attribute(_,Some(Metadata(name,desc)),_,_) => name == "freebase_id"}).get.asInstanceOf[Attribute].values.indexOf(recId1)

            val actualAgeVal = children.find({case Attribute(_,Some(Metadata(name,desc)),_,_) => name == "age"}).get.asInstanceOf[Attribute].values(rec1Idx)
            actualAgeVal mustEqual "" //currently encoding nulls as empty string
        }
    }
    
}