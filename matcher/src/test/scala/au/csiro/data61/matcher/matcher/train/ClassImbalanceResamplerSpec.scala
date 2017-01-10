/**
  * Copyright (C) 2015-2016 Data61, Commonwealth Scientific and Industrial Research Organisation (CSIRO).
  * See the LICENCE.txt file distributed with this work for additional
  * information regarding copyright ownership.
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package au.csiro.data61.matcher.matcher.train

import org.specs2._
import au.csiro.data61.matcher.data._
import com.typesafe.scalalogging.LazyLogging

import language.postfixOps
import scala.collection.JavaConverters._

class ClassImbalanceResamplerSpec extends mutable.Specification with LazyLogging {
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

    val bc1 = new Attribute("phone1", None, (1 to 10).map(_.toString).toList, Some(parent))
    val bc2 = new Attribute("phone2", None, (11 to 20).map(_.toString).toList, Some(parent))
    val bc3 = new Attribute("phone3", None, (1 to 10).map(_.toString).toList, Some(parent))
    val bc4 = new Attribute("address1", None, ('a' to 'k').map(_.toString).toList, Some(parent))
    val bc5 = new Attribute("address2", None, ('e' to 'm').map(_.toString).toList, Some(parent))
    val bc6 = new Attribute("address3", None, ('a' to 'm').map(_.toString).toList, Some(parent))
    val bc7 = new Attribute("address4", None, ('k' to 'z').map(_.toString).toList, Some(parent))
    val bc8 = new Attribute("email1", None, ('a' to 'd').map(_.toString).toList, Some(parent))
    val bc9 = new Attribute("email2", None, ('a' to 'o').map(_.toString).toList, Some(parent))
    val bc10 = new Attribute("unknown1", None, List("1","2"), Some(parent))
    val bc11 = new Attribute("unknown2", None, List("5","6"), Some(parent))
    val bc12 = new Attribute("unknown3", None, List("1","2","3"), Some(parent))
    val bc13 = new Attribute("unknown4", None, List("1","2","3"), Some(parent))
    val bc14 = new Attribute("unknown5", None, List("1","2","3"), Some(parent))
    val bc15 = new Attribute("unknown6", None, List("1","2","3"), Some(parent))
    val bc16 = new Attribute("occupation1", None, ('a' to 'd').map(_.toString).toList, Some(parent))
    val bc17 = new Attribute("occupation2", None, ('k' to 'z').map(_.toString).toList, Some(parent))
    val bc18 = new Attribute("occupation3", None, ('a' to 'd').map(_.toString).toList, Some(parent))
    val bc19 = new Attribute("occupation4", None, ('a' to 'd').map(_.toString).toList, Some(parent))
    val bc20 = new Attribute("occupation5", None, ('a' to 'd').map(_.toString).toList, Some(parent))

    val labels = SemanticTypeLabels(Map(
        "phone1" -> ManualSemanticTypeLabel("phone1", "phone"),
        "phone2" -> ManualSemanticTypeLabel("phone2", "phone"),
        "phone3" -> ManualSemanticTypeLabel("phone3", "phone"),
        "address1" -> ManualSemanticTypeLabel("address1", "address"),
        "address2" -> ManualSemanticTypeLabel("address2", "address"),
        "address3" -> ManualSemanticTypeLabel("address3", "address"),
        "address4" -> ManualSemanticTypeLabel("address4", "address"),
        "email1" -> ManualSemanticTypeLabel("email1", "email"),
        "email2" -> ManualSemanticTypeLabel("email2", "email"),
        "unknown1" -> ManualSemanticTypeLabel("unknown1", "unknown"),
        "unknown2" -> ManualSemanticTypeLabel("unknown2", "unknown"),
        "unknown3" -> ManualSemanticTypeLabel("unknown3", "unknown"),
        "unknown4" -> ManualSemanticTypeLabel("unknown4", "unknown"),
        "unknown5" -> ManualSemanticTypeLabel("unknown5", "unknown"),
        "unknown6" -> ManualSemanticTypeLabel("unknown6", "unknown"),
        "occupation1" -> ManualSemanticTypeLabel("occupation1", "job"),
        "occupation2" -> ManualSemanticTypeLabel("occupation2", "job"),
        "occupation3" -> ManualSemanticTypeLabel("occupation3", "job"),
        "occupation4" -> ManualSemanticTypeLabel("occupation4", "job"),
        "occupation5" -> ManualSemanticTypeLabel("occupation5", "job")
    ))

    val attrList = List(c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11,c12,c13,c14,c15,c16,c17,c18,c19,c20)
    val fullAttrList = List(bc1,bc2,bc3,bc4,bc5,bc6,bc7,bc8,bc9,bc10,bc11,bc12,bc13,bc14,bc15,bc16,bc17,bc18,bc19,bc20)
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

    s"""ClassImbalanceResampler bagging unknown""" should {
        "perform bagging with no resampling for unknown class label" in {
            val valAttrList = List(bc10,bc11,bc12,bc13,bc14,bc15)
            logger.info(s"Num attributes: ${valAttrList.size}")
            val result1 = ClassImbalanceResampler.bagging("None", valAttrList, labels, 3, 3)
            logger.info(s"Num resampled: ${result1.size}")
            result1.size mustEqual 18
            result1.forall(attr => attr.values.size == 3) mustEqual true
            result1.groupBy(_.id).forall { case (attrId, instances) => instances.size == 3 } mustEqual true

        }
    }

    s"""ClassImbalanceResampler bagging all""" should {
        "perform bagging with no resampling for all attributes" in {
            val valAttrList = fullAttrList
            logger.info(s"Num attributes: ${valAttrList.size}")
            val result1 = ClassImbalanceResampler.bagging("None", valAttrList, labels, 3, 3)
            logger.info(s"Num resampled: ${result1.size}")
            result1.size mustEqual 60
            result1.forall(attr => attr.values.size == 3) mustEqual true
            result1.groupBy(_.id).forall { case (attrId, instances) => instances.size == 3 } mustEqual true

        }
    }

    s"""ClassImbalanceResampler bagging unknown with resampletomax""" should {
        "perform bagging with resampling to max for uknown" in {
            val valAttrList = List(bc10,bc11,bc12,bc13,bc14,bc15)
            logger.info(s"Num attributes: ${valAttrList.size}")
            val result1 = ClassImbalanceResampler.bagging("ResampleToMax", valAttrList, labels, 3, 3)
            result1.size mustEqual 18
            result1.forall(attr => attr.values.size == 3) mustEqual true
            result1.groupBy(_.id).forall { case (attrId, instances) => instances.size == 3 } mustEqual true
        }
    }

    s"""ClassImbalanceResampler bagging unkown with resampletomean""" should {
        "perform bagging with resampling to mean for unknown" in {
            val valAttrList = List(bc10,bc11,bc12,bc13,bc14,bc15)
            logger.info(s"Num attributes: ${valAttrList.size}")
            val result1 = ClassImbalanceResampler.bagging("ResampleToMean", valAttrList, labels, 3, 3)
            result1.size mustEqual 18
            result1.forall(attr => attr.values.size == 3) mustEqual true
            result1.groupBy(_.id).forall { case (attrId, instances) => instances.size == 3 } mustEqual true
        }
    }

    s"""ClassImbalanceResampler bagging with resampletomax""" should {
        "perform bagging with resampling to max for all cols" in {
            val valAttrList = fullAttrList
            logger.info(s"Num attributes: ${valAttrList.size}")
            val result1 = ClassImbalanceResampler.bagging("ResampleToMax", valAttrList, labels, 3, 3)
            result1.size mustEqual 90
            result1.forall(attr => attr.values.size == 3) mustEqual true
            result1.groupBy(_.id).forall { case (attrId, instances) => instances.size == 3 } mustEqual false
        }
    }

    s"""ClassImbalanceResampler bagging with resampletomean""" should {
        "perform bagging with resampling to mean for all cols" in {
            val valAttrList = fullAttrList
            logger.info(s"Num attributes: ${valAttrList.size}")
            val result1 = ClassImbalanceResampler.bagging("ResampleToMean", valAttrList, labels, 3, 3)
            result1.size mustEqual 60
            result1.forall(attr => attr.values.size == 3) mustEqual true
            result1.groupBy(_.id).forall { case (attrId, instances) => instances.size == 3 } mustEqual false
        }
    }
}