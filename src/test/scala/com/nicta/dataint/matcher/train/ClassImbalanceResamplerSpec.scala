package com.nicta.dataint.matcher.train

import org.specs2._
import com.nicta.dataint.data._

import scala.collection.JavaConverters._

class ClassImbalanceResamplerSpec extends mutable.Specification {
    val c1 = new Attribute("phone1", None, List(), Some(parent))
    val c2 = new Attribute("phone2", None, List(), Some(parent))
    val c3 = new Attribute("phone3", None, List(), Some(parent))
    val c4 = new Attribute("address1", None, List(), Some(parent))
    val c5 = new Attribute("address2", None, List(), Some(parent))
    val c6 = new Attribute("address3", None, List(), Some(parent))
    val c7 = new Attribute("address4", None, List(), Some(parent))
    val c8 = new Attribute("email1", None, List(), Some(parent))
    val c9 = new Attribute("email2", None, List(), Some(parent))
    val c10 = new Attribute("unknown1", None, List(), Some(parent))
    val c11 = new Attribute("unknown2", None, List(), Some(parent))
    val c12 = new Attribute("unknown3", None, List(), Some(parent))
    val c13 = new Attribute("unknown4", None, List(), Some(parent))
    val c14 = new Attribute("unknown5", None, List(), Some(parent))
    val c15 = new Attribute("unknown6", None, List(), Some(parent))
    val c16 = new Attribute("occupation1", None, List(), Some(parent))
    val c17 = new Attribute("occupation2", None, List(), Some(parent))
    val c18 = new Attribute("occupation3", None, List(), Some(parent))
    val c19 = new Attribute("occupation4", None, List(), Some(parent))
    val c20 = new Attribute("occupation5", None, List(), Some(parent))

    val labels = SemanticTypeLabels(Map(
        ("phone1" -> ManualSemanticTypeLabel("phone1", "phone")),
        ("phone2" -> ManualSemanticTypeLabel("phone2", "phone")),
        ("phone3" -> ManualSemanticTypeLabel("phone3", "phone")),
        ("address1" -> ManualSemanticTypeLabel("address1", "address")),
        ("address2" -> ManualSemanticTypeLabel("address2", "address")),
        ("address3" -> ManualSemanticTypeLabel("address3", "address")),
        ("address4" -> ManualSemanticTypeLabel("address4", "address")),
        ("email1" -> ManualSemanticTypeLabel("email1", "email")),
        ("email2" -> ManualSemanticTypeLabel("email2", "email")),
        ("unknown1" -> ManualSemanticTypeLabel("unknown1", "unknown")),
        ("unknown2" -> ManualSemanticTypeLabel("unknown2", "unknown")),
        ("unknown3" -> ManualSemanticTypeLabel("unknown3", "unknown")),
        ("unknown4" -> ManualSemanticTypeLabel("unknown4", "unknown")),
        ("unknown5" -> ManualSemanticTypeLabel("unknown5", "unknown")),
        ("unknown6" -> ManualSemanticTypeLabel("unknown6", "unknown")),
        ("occupation1" -> ManualSemanticTypeLabel("occupation1", "job")),
        ("occupation2" -> ManualSemanticTypeLabel("occupation2", "job")),
        ("occupation3" -> ManualSemanticTypeLabel("occupation3", "job")),
        ("occupation4" -> ManualSemanticTypeLabel("occupation4", "job")),
        ("occupation5" -> ManualSemanticTypeLabel("occupation5", "job"))
    ))

    val attrList = List(c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18,c19,c20)
    lazy val parent: DataModel = new DataModel("parent1", 
                            Some(Metadata("p1", "parent table")), 
                            None,
                            Some(attrList))

    s"""ClassImbalanceResampler resampleMax""" should {
        "resample each class to max" in {
            val result = ClassImbalanceResampler.resample("UpsampleToMax", attrList, labels)
            result.size mustEqual 30
        }
    }

    s"""ClassImbalanceResampler resampleMean""" should {
        "resample each class to the mean" in {
            val result1 = ClassImbalanceResampler.resample("ResampleToMean", attrList, labels)
            result1.size mustEqual 20
            val result2 = ClassImbalanceResampler.resample("UpsampleToMean", attrList, labels)
            result2.size mustEqual 23
        }
    }

    s"""ClassImbalanceResampler resampleMedian""" should {
        "resample each class to the median" in {
            val result1 = ClassImbalanceResampler.resample("ResampleToMedian", attrList, labels)
            result1.size mustEqual 20
            val result2 = ClassImbalanceResampler.resample("UpsampleToMedian", attrList, labels)
            result2.size mustEqual 23
        }
    }
}