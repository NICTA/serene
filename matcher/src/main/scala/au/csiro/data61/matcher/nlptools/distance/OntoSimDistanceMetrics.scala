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
package au.csiro.data61.matcher.nlptools.distance

import fr.inrialpes.exmo.ontosim.string._

object OntoSimDistanceMetrics {
  def computeDistance(algname: String): (String,String) => Double = algname match {
    case "NeedlemanWunschDistance" =>
      (s1:String,s2:String) => StringDistances.needlemanWunschDistance(s1.toLowerCase,s2.toLowerCase,2)
    case "NGramDistance" => (s1:String,s2:String) => {
      val dist = StringDistances.ngramDistance(s1.toLowerCase,s2.toLowerCase)
      if(!java.lang.Double.isNaN(dist)) dist else 0.0
    }
    case "JaroMeasure" => (s1:String,s2:String) => StringDistances.jaroMeasure(s1.toLowerCase,s2.toLowerCase)
    case _ => (s1:String,s2:String) => {if(s1 equalsIgnoreCase s2) 0.0 else 1.0}
  }
}