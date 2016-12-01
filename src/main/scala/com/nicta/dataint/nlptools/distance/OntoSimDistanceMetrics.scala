package com.nicta.dataint.nlptools.distance

import fr.inrialpes.exmo.ontosim.string._

object OntoSimDistanceMetrics {
    def computeDistance(algname: String): (String,String) => Double = algname match {
        case "NeedlemanWunschDistance" => (s1:String,s2:String) => StringDistances.needlemanWunschDistance(s1.toLowerCase,s2.toLowerCase,2)
        case "NGramDistance" => (s1:String,s2:String) => {
            val dist = StringDistances.ngramDistance(s1.toLowerCase,s2.toLowerCase)
            if(!java.lang.Double.isNaN(dist)) dist else 0.0
        }
        case "JaroMeasure" => (s1:String,s2:String) => StringDistances.jaroMeasure(s1.toLowerCase,s2.toLowerCase)
        case _ => (s1:String,s2:String) => {if(s1 equalsIgnoreCase s2) 0.0 else 1.0}
    }
}