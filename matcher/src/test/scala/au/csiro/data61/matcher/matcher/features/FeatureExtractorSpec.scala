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


package au.csiro.data61.matcher.matcher.features

import org.specs2._
import au.csiro.data61.matcher.data._

import scala.io._

class FeatureExtractorSpec extends mutable.Specification {
    s"""NumUniqueValuesFeatureExtractor""" should {
         "count number of unique values" in {
         	val dummyParent = new DataModel("dummyParentId",None,None,None)
             val attr = Attribute("testAttr", None, List("asdf","foobar","foobaz","asdf"),Some(dummyParent))
             NumUniqueValuesFeatureExtractor().computeFeature(PreprocessedAttribute(attr, Map())) mustEqual 3
         }
    }

    s"""NumericalCharRatioFeatureExtractor""" should {
         "compute ratios" in {
         	val extractor = NumericalCharRatioFeatureExtractor()
         	val dummyParent = new DataModel("dummyParentId",None,None,None)
             val attr1 = Attribute("testAttr", None, List("1asdf","foba0","o9baz","a8sdf"),Some(dummyParent))
             extractor.computeFeature(PreprocessedAttribute(attr1,Map())) mustEqual 0.2

             val attr2 = Attribute("testAttr", None, List("","foba0","o9baz","a8sdf"),Some(dummyParent))
             extractor.computeFeature(PreprocessedAttribute(attr2,Map())) mustEqual 0.15000000000000002

             val attr3 = Attribute("testAttr", None, List("","","",""),Some(dummyParent))
             extractor.computeFeature(PreprocessedAttribute(attr3,Map())) mustEqual 0
         }
     }

     s"""WhitespaceRatioFeatureExtractor""" should {
         "compute ratios" in {
         	val extractor = WhitespaceRatioFeatureExtractor()
         	val dummyParent = new DataModel("dummyParentId",None,None,None)
             val attr1 = Attribute("testAttr", None, List(" asdf","foba ","o baz","a sdf"),Some(dummyParent))
             extractor.computeFeature(PreprocessedAttribute(attr1,Map())) mustEqual 0.2

             val attr2 = Attribute("testAttr", None, List("","foba ","o baz","a sdf"),Some(dummyParent))
             extractor.computeFeature(PreprocessedAttribute(attr2,Map())) mustEqual 0.15000000000000002

             val attr3 = Attribute("testAttr", None, List("","","",""),Some(dummyParent))
             extractor.computeFeature(PreprocessedAttribute(attr3,Map())) mustEqual 0
         }
     }

//     s"""TermCosineSimilarityFeatureExtractor test1""" should {
//         "compute features" in {
//             val attr1Contents = Source.fromInputStream(
//               getClass.getClassLoader.getResourceAsStream("datasets/northix/db1/return_date@rental@1.dat"))
//               .getLines.toList
//             val attr2Contents = Source.fromInputStream(
//               getClass.getClassLoader.getResourceAsStream("datasets/northix/db2/UnitsOnOrder@products@2.txt")).getLines.toList
//
//             val extractor = TermCosineSimilarityFeatureExtractor()
//             val dummyParent = new DataModel("dummyParentId",None,None,None)
//             val attr1 = Attribute("testAttr", None, attr1Contents,Some(dummyParent))
//             val attr2 = Attribute("testAttr", None, attr2Contents,Some(dummyParent))
//
//             val preprocessor = DataPreprocessor()
//             val List(pAttr1,pAttr2) = preprocessor.preprocess(List(attr1,attr2))
//
//             val fe = TermCosineSimilarityFeatureExtractor()
//             val features = fe.computeFeature(pAttr1,pAttr2)
//             features mustEqual List(0.0)
//
//         }
//     }
//
//     s"""TermCosineSimilarityFeatureExtractor test2""" should {
//         "compute features" in {
//             val attr1Contents = Source.fromInputStream(
//               getClass.getClassLoader.getResourceAsStream("datasets/northix/db1/active@customer@1.dat"))
//               .getLines.toList
//             val attr2Contents = Source.fromInputStream(
//               getClass.getClassLoader.getResourceAsStream("datasets/northix/db2/EmployeeID@orders@2.txt"))
//               .getLines.toList
//
//             val extractor = TermCosineSimilarityFeatureExtractor()
//             val dummyParent = new DataModel("dummyParentId",None,None,None)
//             val attr1 = Attribute("testAttr", None, attr1Contents,Some(dummyParent))
//             val attr2 = Attribute("testAttr", None, attr2Contents,Some(dummyParent))
//
//             val preprocessor = DataPreprocessor()
//             val List(pAttr1,pAttr2) = preprocessor.preprocess(List(attr1,attr2))
//
//             val attr1Tf = pAttr1.preprocessedDataMap("normalised-term-frequency-vector").asInstanceOf[Map[String,Double]]
//             val attr2Tf = pAttr2.preprocessedDataMap("normalised-term-frequency-vector").asInstanceOf[Map[String,Double]]
//
//             println("attr1Tf: " + attr1Tf)
//             println("attr2Tf: " + attr2Tf)
//
//             val fe = TermCosineSimilarityFeatureExtractor()
//             val features = fe.computeFeature(pAttr1,pAttr2)
//             features mustEqual List(0.4126935417380277) //kinda too high actually
//
//         }
//     }

    s"""WUPWordNetFeatureExtractor""" should {
        "compute ratios" in {
            val extractor = JCNWordNetFeatureExtractor()
            val dummyParent = new DataModel("dummyParentId",None,None,None)
            val attr1 = Attribute("testAttr", Some(Metadata("amount", "")), List(),Some(dummyParent))
            val attr2 = Attribute("testAttr", Some(Metadata("OrderDate","")), List(),Some(dummyParent))

            val preprocessor = DataPreprocessor()
            val List(pAttr1,pAttr2) = preprocessor.preprocess(List(attr1,attr2))

            val fe = JCNWordNetFeatureExtractor()
            val result = fe.computeFeature(pAttr1,pAttr2)

            println(result)

            1 mustEqual 1
        }
    }

//     s"""WUPWordNetFeatureExtractor""" should {
//         "compute ratios" in {
//             val extractor = WUPWordNetFeatureExtractor()
//             val dummyParent = new DataModel("dummyParentId",None,None,None)
//             val attr1 = Attribute("testAttr", Some(Metadata("staff_id", "")), List(),Some(dummyParent))
//             val attr2 = Attribute("testAttr", Some(Metadata("EmployeeID","")), List(),Some(dummyParent))
//
//             val preprocessor = DataPreprocessor()
//             val List(pAttr1,pAttr2) = preprocessor.preprocess(List(attr1,attr2))
//
//             val fe = WUPWordNetFeatureExtractor()
//             val result = fe.computeFeature(pAttr1,pAttr2)
//
//             println(result)
//
//             1 mustEqual 1
//         }
//     }

//     s"""StringLengthStatisticsFeatureExtractor""" should {
//         "compute string length statistics" in {
//         	val extractor = StringLengthStatisticsFeatureExtractor()
//         	val dummyParent = new DataModel("dummyParentId",None,None,None)
//             val attr1 = Attribute("testAttr", None, List(" asdff 81l","_)1 fjiaje","o baza","1a sdf"),Some(dummyParent))
//             val stats1 = extractor.computeFeature(attr1).asInstanceOf[BasicStats]
//             stats1.mean mustEqual 8
//
//             val attr2 = Attribute("testAttr", None, List("abc","def","hij","klmnop","qrs", "tuvwxyz "),Some(dummyParent))
//             val stats2 = extractor.computeFeature(attr2).asInstanceOf[BasicStats]
//             stats2.mode mustEqual 3
//
//             val attr3 = Attribute("testAttr", None, List("abc","def","hij","123456","qrs87 a", "ofa7a &", "oaljf8e"),Some(dummyParent))
//             val stats3 = extractor.computeFeature(attr3).asInstanceOf[BasicStats]
//             stats3.median mustEqual 6
//
//             val attr4 = Attribute("testAttr", None, List("ad", "abc","def","hij","123456","qrs87 a", "ofa7a &", "oaljf8e"),Some(dummyParent))
//             val stats4 = extractor.computeFeature(attr4).asInstanceOf[BasicStats]
//             stats4.median mustEqual 3
//             stats4.min mustEqual 2
//             stats4.max mustEqual 7
//         }
//     }
//
//     s"""NumericalAttributeStatisticsFeatureExtractor""" should {
//         "compute string length statistics" in {
//         	val extractor = NumericalAttributeStatisticsFeatureExtractor()
//         	val dummyParent = new DataModel("dummyParentId",None,None,None)
//             val attr1 = Attribute("testAttr", None, List("10.0","10","6.00","06."),Some(dummyParent))
//             val stats1 = extractor.computeFeature(attr1).asInstanceOf[BasicStats]
//             stats1.mean mustEqual 8
//
//             val attr2 = Attribute("testAttr", None, List("3","3","3","6","3", "8"),Some(dummyParent))
//             val stats2 = extractor.computeFeature(attr2).asInstanceOf[BasicStats]
//             stats2.mode mustEqual 3
//
//             val attr3 = Attribute("testAttr", None, List("3","3","3","6","7", "7", "7"),Some(dummyParent))
//             val stats3 = extractor.computeFeature(attr3).asInstanceOf[BasicStats]
//             stats3.median mustEqual 6
//
//             val attr4 = Attribute("testAttr", None, List("2", "3","3","3","6","7", "7", "7"),Some(dummyParent))
//             val stats4 = extractor.computeFeature(attr4).asInstanceOf[BasicStats]
//             stats4.median mustEqual 3
//             stats4.min mustEqual 2
//             stats4.max mustEqual 7
//         }
//     }
}