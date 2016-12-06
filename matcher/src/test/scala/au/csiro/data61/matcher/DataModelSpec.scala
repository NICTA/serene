package au.csiro.data61.matcher.data

import org.specs2._
import au.csiro.data61.matcher.data._
import au.csiro.data61.matcher.ingestion.loader._

class DataModelSpec extends mutable.Specification {
    
    s"""DataModel constructor""" should {
        """should create DataModel with id "dm1" """ in {
            val dm = new DataModel("dm1", Some(Metadata("table1", "table1 desc")), None, None) 
            dm.id mustEqual "dm1"
        }
    }

    s"""DataModel hierarchy""" should {
        """child's parent id must be "parent1" """ in {
            lazy val c1: DataModel = new DataModel("child1", 
                                        Some(Metadata("c1", "child table")), 
                                        Some(p1), 
                                        None)

            lazy val p1: DataModel = new DataModel("parent1", 
                                        Some(Metadata("p1", "parent table")), 
                                        None,
                                        Some(List(c1)))
            
            val id = c1.parent match {
                case Some(x: DataModel) => x.id
                case _ => "fail"
            } 
            id mustEqual "parent1"
        }
    }

    s"""DataModelAttribute constructor""" should {
        """should create Attribute with parent id "table1" """ in {
            lazy val table: DataModel = new DataModel("table1", 
                                                      Some(Metadata("table1", "table1 desc")), 
                                                      None, Some(List(attr1)))
            lazy val attr1: Attribute = new Attribute("attr1", 
                                                       Some(Metadata("attr1", "attribute1 desc")),
                                                       List(""), 
                                                       Some(table)) 
            val pid = attr1.parent match {
                case Some(x: DataModel) => x.id
                case _ => "fail"
            } 
            pid mustEqual "table1"

            val cid = table.children match {
                case Some(List(x: Attribute)) => x.id
                case _ => "fail"
            }
            cid mustEqual "attr1"
        }
    }

    s"""DataModel copy() for attributes (attr1,attr3)""" should {
        """create a DataModel copy with only these attributes """ in {
            lazy val c1: Attribute = new Attribute("attr1", 
                                        Some(Metadata("c1", "child attribute 1")),
                                        List(),
                                        Some(p1))
            lazy val c2: Attribute = new Attribute("attr2", 
                                        Some(Metadata("c2", "child attribute 2")), 
                                        List(),
                                        Some(p1))
            lazy val c3: Attribute = new Attribute("attr3", 
                                        Some(Metadata("c3", "child attribute 3")), 
                                        List(),
                                        Some(p1))

            lazy val p1: DataModel = new DataModel("parent1", 
                                        Some(Metadata("p1", "parent table")), 
                                        None,
                                        Some(List(c1,c2,c3)))
            
            lazy val p1c: DataModel = DataModel.copy(p1, None, Set("attr1","attr3"))

            val childrenCopy = p1c.children.get.map(_.id).toSet

            childrenCopy must contain("attr1")
            childrenCopy must contain("attr3")
            childrenCopy must not contain("attr2")
        }
    }

    s"""DataModel copy() for northix smalldb1only (create_date,email)""" should {
        """create a DataModel copy with only these attributes """ in {
            val sdb1 = NorthixDataLoader("NorthixDB").load("src/test/resources/datasets/northix/smalldb1only")
            val attrs = DataModel.getAllAttributes(sdb1)
            attrs.size mustEqual 5

            val sdb1Copy = DataModel.copy(sdb1, None, Set("create_date@customer@1.dat","email@customer@1.dat"))
            val attrsCopy = DataModel.getAllAttributes(sdb1Copy).map(_.id)

            attrsCopy.size mustEqual 2
            attrsCopy must contain("create_date@customer@1.dat")
            attrsCopy must contain("email@customer@1.dat")
        }
    }
}