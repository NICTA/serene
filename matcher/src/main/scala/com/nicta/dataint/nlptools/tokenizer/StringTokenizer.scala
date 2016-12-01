package com.nicta.dataint.nlptools.tokenizer

import scala.io._

import com.nicta.dataint.util._

object StringTokenizer {
    lazy val dictionary = Source.fromInputStream(getClass.getClassLoader.getResourceAsStream("dict/infochimps/words.txt")).getLines.toSet

    val nonAlphaNumSplitter = "[^a-zA-Z0-9]"
    val camelCaseSplitter = "(?<!(^|[A-Z]))(?=[A-Z])|(?<!^)(?=[A-Z][a-z])"

    /**
     *  This method splits String s into the minimum number of english words
     *  that it is composed of.
     **/
    def splitCompoundedWords(s: String): List[String] = {
        lazy val getLongestWord: Memoized1[String, String] = Memoized1(
            (s:String) => {
                if(s.length <= 1) ""
                else {
                    if(dictionary contains s) s
                    else {
                        val w1 = getLongestWord(s.substring(1))
                        val w2 = getLongestWord(s.substring(0,s.length-1))
                        if(w1.length > w2.length) w1
                        else w2
                    }
                }
            }
        )

        lazy val splitCompoundedWordsMemoized: Memoized1[String, List[String]] = Memoized1(
            (s:String) => {
                if(s.length == 0) List()
                else {
                    val w = getLongestWord(s)
                    if(w.length > 0) {
                        val wIdx = s.indexOf(w)
                        splitCompoundedWordsMemoized(s.substring(0,wIdx)) ++ List(w) ++ splitCompoundedWordsMemoized(s.substring(wIdx+w.length))
                    } else {
                        List(s)       
                    }
                }
            }
        )
        splitCompoundedWordsMemoized(s)
    }    

    def splitByCase(s: String): List[String] = {
        s.split(camelCaseSplitter).toList
    }

    def splitByCharacter(s: String): List[String] = {
        s.split(nonAlphaNumSplitter).toList
    }

    def tokenize(s: String): List[String] = {
        StringTokenizer.splitByCase(s).flatMap(StringTokenizer.splitByCharacter).map({_.toLowerCase}).flatMap(StringTokenizer.splitCompoundedWords).toList
    }
}