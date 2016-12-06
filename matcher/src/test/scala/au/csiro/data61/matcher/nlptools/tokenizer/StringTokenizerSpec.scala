package au.csiro.data61.matcher.nlptools.tokenizer

import org.specs2._
import au.csiro.data61.matcher.nlptools.tokenizer._

class StringTokenizerSpec extends mutable.Specification {
    """StringTokenizer splitCompoundedWords("personaldetails")""" should {

        val testStr1 = "personaldetails"
        val expectedResults1 = List("personal", "details")
        s"""split $testStr1 into $expectedResults1 """ in {
            (StringTokenizer.dictionary contains "personal") mustEqual true
            (StringTokenizer.dictionary contains "details") mustEqual true
            StringTokenizer.splitCompoundedWords(testStr1) mustEqual expectedResults1
        }

        val testStr2 = "shippingaddress"
        val expectedResults2 = List("shipping","address")
        s"""split $testStr2 into $expectedResults2 """ in {
            (StringTokenizer.dictionary contains "shipping") mustEqual true
            (StringTokenizer.dictionary contains "address") mustEqual true
            StringTokenizer.splitCompoundedWords(testStr2) mustEqual expectedResults2
        }

        val testStr3 = "xywaddress"
        val expectedResults3 = List("xyw","address")
        s"""split $testStr3 into $expectedResults3 """ in {
            (StringTokenizer.dictionary contains "address") mustEqual true
            (StringTokenizer.dictionary contains "xyw") mustEqual false
            StringTokenizer.splitCompoundedWords(testStr3) mustEqual expectedResults3
        }

        val testStr4 = "ship2address"
        val expectedResults4 = List("ship","2","address")
        s"""split $testStr4 into $expectedResults4 """ in {
            StringTokenizer.splitCompoundedWords(testStr4) mustEqual expectedResults4
        }            

        val testStr5 = "valid142p"
        val expectedResults5 = List("valid", "142p")
        s"""split $testStr5 into $expectedResults5 """ in {
            (StringTokenizer.dictionary contains "valid") mustEqual true
            (StringTokenizer.dictionary contains "142p") mustEqual false
            StringTokenizer.splitCompoundedWords(testStr5) mustEqual expectedResults5
        }

        val testStr6 = "valid142pdata"
        val expectedResults6 = List("valid", "142p", "data")
        s"""split $testStr6 into expectedResults6 """ in {
            (StringTokenizer.dictionary contains "valid") mustEqual true
            (StringTokenizer.dictionary contains "data") mustEqual true
            StringTokenizer.splitCompoundedWords(testStr6) mustEqual expectedResults6
        }  

        val testStr7 = "deliveryxywxywxywxywxywxywxywaddress"
        val expectedResults7 = List("delivery", "xywxywxywxywxywxywxyw", "address")
        s"""split $testStr7 into $expectedResults7 """ in {
            (StringTokenizer.dictionary contains "delivery") mustEqual true
            (StringTokenizer.dictionary contains "address") mustEqual true
            StringTokenizer.splitCompoundedWords(testStr7) mustEqual expectedResults7
        }

        val testStr8 = "deliveryxywaddress"
        val expectedResults8 = List("delivery", "xyw", "address")
        s"""split $testStr8 into $expectedResults8 """ in {
            (StringTokenizer.dictionary contains "delivery") mustEqual true
            (StringTokenizer.dictionary contains "address") mustEqual true
            StringTokenizer.splitCompoundedWords(testStr8) mustEqual expectedResults8
        }

        val testStr9 = "deliveryid"
        val expectedResults9 = List("delivery", "id")
        s"""split $testStr9 into $expectedResults9 """ in {
            (StringTokenizer.dictionary contains "delivery") mustEqual true
            (StringTokenizer.dictionary contains "id") mustEqual true
            StringTokenizer.splitCompoundedWords(testStr9) mustEqual expectedResults9
        }

        // //TODO: this fails
        // val testStr10 = "personsalary_grossAnnual"
        // val expectedResults10 = List("person", "salary", "gross", "Annual")
        // s"""split $testStr10 into $expectedResults10 """ in {
        //     (StringTokenizer.dictionary contains "annual") mustEqual true
        //     StringTokenizer.tokenize(testStr10) mustEqual expectedResults10
        // }

        val testStr11 = "personalincome_grossAnnual"
        val expectedResults11 = List("personal", "income", "gross", "annual")
        s"""split $testStr11 into $expectedResults11 """ in {
            StringTokenizer.tokenize(testStr11) mustEqual expectedResults11
        }

        s"""split $testStr4 into $expectedResults4 """ in {
            StringTokenizer.tokenize(testStr4) mustEqual expectedResults4
        }                     
    }
}