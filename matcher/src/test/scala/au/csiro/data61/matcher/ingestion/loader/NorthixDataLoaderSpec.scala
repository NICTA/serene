package au.csiro.data61.matcher.ingestion.loader

import org.specs2._
import au.csiro.data61.matcher.data._
import au.csiro.data61.matcher.ingestion.loader._

class NorthixDataLoaderSpec extends mutable.Specification {
    val smallNorthixData = "src/test/resources/datasets/northix/small"
    val db1 = "src/test/resources/datasets/northix/db1"
    val db2 = "src/test/resources/datasets/northix/db2"
    val gtPath = "src/test/resources/datasets/northix/gt"

    s"""NorthixDataLoader loadLabeledData("$gtPath")""" should {
        "load ground truth mappings" in {
            val mappings = NorthixDataLoader("NorthixDB").loadLabels(gtPath) match {
                case l: BasicLabels => l.positiveLabels.toList
                case _ => List()
            }

            mappings.size mustEqual 33

            val foundResults1 = mappings.map(s => s.contains("OrderID@orders@2.txt")
                                                  && s.contains("OrderID@order_details@2.txt")
                                                  && s.contains("odID@order_details@2.txt")).foldLeft(false)(_||_)
            foundResults1 mustEqual true

            val foundResults2 = mappings.map(s => s.contains("amount@payment@1.dat")
                                                  && s.contains("amount@order@2.txt")).foldLeft(false)(_||_)
            foundResults2 mustEqual true
        }
    }

    s"""NorthixDataLoader load("$smallNorthixData")""" should {
        s"read 2 databases with a total of 3 tables from $smallNorthixData" in {
            val dataset = NorthixDataLoader("NorthixDB").load(smallNorthixData)
            val datasetMeta: Metadata = dataset.metadata.get

            datasetMeta.name mustEqual "Northix Dataset"
            val dbNames = dataset.children.get.map({
                 case x => x.metadata.get.name
            })
            dbNames must contain("1.dat")
            dbNames must contain("2.txt")

            val tableNames = dataset.children.get.flatMap(db => db.asInstanceOf[DataModel].children.get.map(t => t.metadata.get.name))
            tableNames must contain("actor","customer", "film_actor", "customers")
        }
    }

    s"""NorthixDataLoader load("$db1")""" should {
        s"read Northwind database with a total of N tables from $db1" in {
            val dataset = NorthixDataLoader("NorthixDB").load(db1)
            val datasetMeta: Metadata = dataset.metadata.get

            val tableNames = dataset.children.get.flatMap(db => db.asInstanceOf[DataModel].children.get).map(_.metadata.get.name)
            tableNames.size mustEqual 12
            tableNames must contain("actor", "inventory", "city", "rental", "country",
                                    "film_actor", "film_text", "payment", "customer",
                                    "address", "film_category", "film")
            DataModel.getAllAttributes(dataset).size mustEqual 67
        }
    }

    s"""NorthixDataLoader load("$db2")""" should {
        s"read Sakila database with a total of N tables from $db2" in {
            val dataset = NorthixDataLoader("NorthixDB").load(db2)
            val datasetMeta: Metadata = dataset.metadata.get

            val tableNames = dataset.children.get.flatMap(db => db.asInstanceOf[DataModel].children.get).map(_.metadata.get.name)
            tableNames.size mustEqual 7
            tableNames must contain("products", "order_details", "suppliers", 
                                    "order", "customer", "orders", "customers")

            DataModel.getAllAttributes(dataset).size mustEqual 48
        }
    }

    s"""NorthixDataLoader load("$db2")""" should {
        s"read Sakila database with a total of 7 tables from $db2" in {
            val dataset = NorthixDataLoader("NorthixDB").load(db2)
            dataset.id mustEqual "NorthixDB"

            val dbnames = dataset.children match {
                case Some(children) => children.map(_.id)
                case None => List("")
            }
            dbnames must not contain("1.dat")
            dbnames must contain("2.txt")

            val tablenames = dataset.children match {
                case Some(databases) => databases.flatMap({
                    case db: DataModel => db.children match {
                        case Some(tables) => tables.map(_.metadata.get.name)
                        case _ => List()
                    }
                })
                case _ => List()
            }

            val tableToTest = "customers"
            val custTable = dataset.children.get.head.asInstanceOf[DataModel].children.get.find({case table => table.metadata.get.name == tableToTest}).get.asInstanceOf[DataModel]
            custTable.metadata.get.name mustEqual tableToTest
            val custTableAttrs = custTable.children.get.map({case attr => attr.metadata.get.name})
            custTableAttrs must contain("CustomerID", "Country", "PostalCode", 
                                        "City", "Address", "Region", "Phone", 
                                        "ContactTitle", "ContactName", "Fax")

            tablenames.size mustEqual 7
            tablenames must contain("customer", "customers", "order", "order_details", "orders", "products", "suppliers")

            val countryAttrVal = custTable.children.get.find(_.metadata.get.name == "Country").get.asInstanceOf[Attribute].values
            countryAttrVal.size mustEqual 91
            countryAttrVal must contain("Germany")
            countryAttrVal must contain("Mexico")
            countryAttrVal must contain("UK")
            countryAttrVal must contain("Sweden")
            countryAttrVal must contain("France")
            countryAttrVal must contain("Spain")
            countryAttrVal must contain("Canada")
            countryAttrVal must contain("Argentina")
            countryAttrVal must contain("Switzerland")
            countryAttrVal must contain("Brazil")
            countryAttrVal must contain("Austria")
            countryAttrVal must contain("Portugal")
            countryAttrVal must contain("USA")
            countryAttrVal must contain("Venezuela")
            countryAttrVal must contain("Ireland")
            countryAttrVal must contain("Italy")
            countryAttrVal must contain("Belgium")
            countryAttrVal must contain("Denmark")
            countryAttrVal must contain("Finland")
            countryAttrVal must contain("Poland")
            countryAttrVal must contain("Norway")
        }
    }
}