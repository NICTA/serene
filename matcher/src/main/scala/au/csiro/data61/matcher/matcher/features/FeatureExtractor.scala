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

import au.csiro.data61.matcher.data._
import au.csiro.data61.matcher.matcher.train.TrainAliases._
import au.csiro.data61.matcher.matcher.train.TrainingSettings
import au.csiro.data61.matcher.matcher._
import au.csiro.data61.matcher.nlptools.tokenizer.StringTokenizer
import com.typesafe.scalalogging.LazyLogging

import scala.util._
import scala.io._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

import scala.language.postfixOps

trait FeatureExtractor


trait SingleFeatureExtractor extends FeatureExtractor {
  def getFeatureName(): String
  def computeFeature(attribute: PreprocessedAttribute): Double
  def computeSimpleFeature(attribute: SimpleAttribute): Double
}

trait GroupFeatureExtractor extends FeatureExtractor {
  def getGroupName(): String
  def getFeatureNames(): List[String]
  def computeFeatures(attribute: PreprocessedAttribute): List[Double]
  def computeSimpleFeatures(attribute: SimpleAttribute): List[Double]
}

trait HeaderGroupFeatureExtractor extends GroupFeatureExtractor {
  def getGroupName(): String
  def getFeatureNames(): List[String]
  def computeFeatures(attribute: PreprocessedAttribute): List[Double]
  def computeSimpleFeatures(attribute: SimpleAttribute): List[Double]
}


object NumUniqueValuesFeatureExtractor {
  def getFeatureName() = "num-unique-vals"
}
case class NumUniqueValuesFeatureExtractor() extends SingleFeatureExtractor {
  override def getFeatureName(): String = NumUniqueValuesFeatureExtractor.getFeatureName

  override def computeFeature(attribute: PreprocessedAttribute): Double = {
    attribute.rawAttribute.values.map(_.toLowerCase.trim).distinct.size
  }

  override def computeSimpleFeature(attribute: SimpleAttribute): Double = {
    attribute.values.map(_.toLowerCase.trim).distinct.length
  }
}


object PropUniqueValuesFeatureExtractor {
  def getFeatureName() = "prop-unique-vals"
}
case class PropUniqueValuesFeatureExtractor() extends SingleFeatureExtractor {
  override def getFeatureName(): String = PropUniqueValuesFeatureExtractor.getFeatureName

  override def computeFeature(attribute: PreprocessedAttribute): Double = {
    val values = attribute.rawAttribute.values
    values.map(_.toLowerCase.trim).distinct.size.toDouble / values.size
  }

  override def computeSimpleFeature(attribute: SimpleAttribute): Double = {
    attribute.values.map(_.toLowerCase.trim).distinct.length.toDouble / attribute.values.length
  }
}


object PropMissingValuesFeatureExtractor {
  def getFeatureName(): String = "prop-missing-vals"
}
case class PropMissingValuesFeatureExtractor() extends SingleFeatureExtractor {
  override def getFeatureName(): String = PropMissingValuesFeatureExtractor.getFeatureName

  override def computeFeature(attribute: PreprocessedAttribute): Double = {
    val values = attribute.rawAttribute.values
    values.count(_.trim.length == 0).toDouble / values.size
  }


  override def computeSimpleFeature(attribute: SimpleAttribute): Double = {
    attribute.values.count(_.trim.length == 0).toDouble / attribute.values.length
  }
}


object PropAlphaCharsFeatureExtractor {
  def getFeatureName(): String = "ratio-alpha-chars"
}
case class PropAlphaCharsFeatureExtractor() extends SingleFeatureExtractor {
  override def getFeatureName(): String = PropAlphaCharsFeatureExtractor.getFeatureName

  override def computeFeature(attribute: PreprocessedAttribute): Double = {
    val attrContent = attribute.rawAttribute.values.mkString("")
    if(attrContent.nonEmpty) {
      attrContent.replaceAll("[^a-zA-Z]","").length.toDouble / attrContent.length
    } else {
      -1.0
    }
  }

  override def computeSimpleFeature(attribute: SimpleAttribute): Double = {
    val attrContent = attribute.values.mkString("")
    if(attrContent.nonEmpty) {
      attrContent.replaceAll("[^a-zA-Z]","").length.toDouble / attrContent.length
    } else {
      -1.0
    }
  }
}


object PropEntriesWithAtSign {
  def getFeatureName(): String = "prop-entries-with-at-sign"
}
case class PropEntriesWithAtSign() extends SingleFeatureExtractor {
  override def getFeatureName(): String = PropEntriesWithAtSign.getFeatureName

  override def computeFeature(attribute: PreprocessedAttribute): Double = {
    val attrContent = attribute.rawAttribute.values.filter { x: String => x.nonEmpty }
    if(attrContent.nonEmpty) {
      attrContent.count{
        x:String => x.contains("@")
      }.toDouble / attrContent.size
    } else {
      -1.0
    }
  }

  override def computeSimpleFeature(attribute: SimpleAttribute): Double = {
    val attrContent = attribute.values.filter(_.nonEmpty)
    if(attrContent.nonEmpty) {
      attrContent.count(_.contains("@")).toDouble / attrContent.length
    } else {
      -1.0
    }
  }

}


object PropEntriesWithCurrencySymbol {
  def getFeatureName() = "prop-entries-with-currency-symbol"
}
case class PropEntriesWithCurrencySymbol() extends SingleFeatureExtractor {
  val currencySymbols = List("$","AUD")

  override def getFeatureName(): String = PropEntriesWithCurrencySymbol.getFeatureName

  override def computeFeature(attribute: PreprocessedAttribute): Double = {
    val attrContent = attribute.rawAttribute.values.filter { x: String => x.nonEmpty}
    if(attrContent.nonEmpty) {
      attrContent.count{
        x: String => currencySymbols.exists(x.contains)
      }.toDouble / attrContent.size
    } else {
      -1.0
    }
  }

  override def computeSimpleFeature(attribute: SimpleAttribute): Double = {
    val attrContent = attribute.values.filter(_.nonEmpty)
    if(attrContent.nonEmpty) {
      attrContent.count{
        x: String => currencySymbols.exists(x.contains)
      }.toDouble / attrContent.size
    } else {
      -1.0
    }
  }
}


object PropEntriesWithHyphen {
  def getFeatureName(): String = "prop-entries-with-hyphen"
}
case class PropEntriesWithHyphen() extends SingleFeatureExtractor {
  override def getFeatureName(): String = PropEntriesWithHyphen.getFeatureName

  override def computeFeature(attribute: PreprocessedAttribute): Double = {
    val attrContent = attribute.rawAttribute.values.filter { x: String => x.nonEmpty}
    if(attrContent.nonEmpty) {
      attrContent.count{
        x: String => x.contains("-")
      }.toDouble / attrContent.size
    } else {
      -1.0
    }
  }

  override def computeSimpleFeature(attribute: SimpleAttribute): Double = {
    val attrContent = attribute.values.filter(_.nonEmpty)
    if(attrContent.nonEmpty) {
      attrContent.count(_.contains("-")).toDouble / attrContent.size
    } else {
      -1.0
    }
  }
}

object PropEntriesWithParen {
  def getFeatureName(): String = "prop-entries-with-paren"
}
case class PropEntriesWithParen() extends SingleFeatureExtractor {
  override def getFeatureName(): String = PropEntriesWithParen.getFeatureName

  override def computeFeature(attribute: PreprocessedAttribute): Double = {
    val attrContent = attribute.rawAttribute.values.filter { x: String => x.nonEmpty }
    if(attrContent.nonEmpty) {
      attrContent.count {
        x: String => x.contains("(") || x.contains(")")
      }.toDouble / attrContent.size
    } else {
      -1.0
    }
  }

  override def computeSimpleFeature(attribute: SimpleAttribute): Double = {
    val attrContent = attribute.values.filter(_.nonEmpty)
    if(attrContent.nonEmpty) {
      attrContent.count {
        x: String => x.contains("(") || x.contains(")")
      }.toDouble / attrContent.length
    } else {
      -1.0
    }
  }
}

object MeanCommasPerEntry {
  def getFeatureName(): String = "mean-commas-per-entry"
}
case class MeanCommasPerEntry() extends SingleFeatureExtractor {
  override def getFeatureName(): String = MeanCommasPerEntry.getFeatureName

  override def computeFeature(attribute: PreprocessedAttribute): Double = {
    val attrContent = attribute.rawAttribute.values.filter { x: String => x.nonEmpty}
    if(attrContent.nonEmpty) {
      val counts = attrContent.map(_.count(_ == ',').toDouble)
      counts.sum / counts.size
    } else {
      -1.0
    }
  }

  override def computeSimpleFeature(attribute: SimpleAttribute): Double = {
    val attrContent = attribute.values.filter(_.nonEmpty)
    if(attrContent.nonEmpty) {
      val counts = attrContent.map(_.count(_ == ',').toDouble)
      counts.sum / counts.length
    } else {
      -1.0
    }
  }
}

object MeanForwardSlashesPerEntry {
  def getFeatureName(): String = "mean-forward-slashes-per-entry"
}
case class MeanForwardSlashesPerEntry() extends SingleFeatureExtractor {
  override def getFeatureName(): String = MeanForwardSlashesPerEntry.getFeatureName

  override def computeFeature(attribute: PreprocessedAttribute): Double = {
    val attrContent = attribute.rawAttribute.values.filter { x: String => x.nonEmpty}
    if(attrContent.nonEmpty) {
      val counts = attrContent.map(_.count(_ == '/').toDouble)
      counts.sum / counts.size
    } else {
      -1.0
    }
  }

  override def computeSimpleFeature(attribute: SimpleAttribute): Double = {
    val attrContent = attribute.values.filter(_.nonEmpty)
    if(attrContent.nonEmpty) {
      val counts = attrContent.map(_.count(_ == '/').toDouble)
      counts.sum / counts.length
    } else {
      -1.0
    }
  }
}

object PropRangeFormat {
  def getFeatureName(): String = "prop-range-format"
}
case class PropRangeFormat() extends SingleFeatureExtractor {
  override def getFeatureName(): String = PropRangeFormat.getFeatureName

  override def computeFeature(attribute: PreprocessedAttribute): Double = {
    val rangeFmt = "([0-9]+)-([0-9]+)".r
    val attrContent = attribute.rawAttribute.values.filter { x: String => x.nonEmpty}
    if(attrContent.nonEmpty) {
      val valsWithRangeFmt = attrContent.filter({
        case rangeFmt(start, end) => start.toDouble <= end.toDouble
        case _ => false
      })
      valsWithRangeFmt.size.toDouble / attrContent.size.toDouble
    } else {
      -1.0
    }
  }


  override def computeSimpleFeature(attribute: SimpleAttribute): Double = {
    val rangeFmt = "([0-9]+)-([0-9]+)".r
    val attrContent = attribute.values.filter(_.nonEmpty)
    if(attrContent.nonEmpty) {
      val valsWithRangeFmt = attrContent.filter {
        case rangeFmt(start, end) => start.toDouble <= end.toDouble
        case _ => false
      }
      valsWithRangeFmt.length.toDouble / attrContent.length.toDouble
    } else {
      -1.0
    }
  }
}


/**
  *  This feature was taken from the paper: "Semantic Integration in Heterogenous
  *  Databases Using Neural Networks" by Wen-Syan and Chris Clifton.
  **/
object NumericalCharRatioFeatureExtractor {
  def getFeatureName = "prop-numerical-chars"
}
case class NumericalCharRatioFeatureExtractor() extends SingleFeatureExtractor {
  override def getFeatureName() = NumericalCharRatioFeatureExtractor.getFeatureName

  override def computeFeature(attribute: PreprocessedAttribute): Double = {
    val numRegex = "[0-9]".r
    val ratios = attribute.rawAttribute.values.map {
      case s if s.length > 0 => numRegex.findAllIn(s).size.toDouble / s.length
      case _ => 0.0
    }
    ratios.sum / ratios.size
  }

  override def computeSimpleFeature(attribute: SimpleAttribute): Double = {
    val numRegex = "[0-9]".r
    val ratios: Array[Double] = attribute.values.map {
      case s if s.nonEmpty => numRegex.findAllIn(s).size.toDouble / s.length
      case _ => 0.0
    }
    ratios.sum / ratios.length
  }
}


object WhitespaceRatioFeatureExtractor {
  def getFeatureName = "prop-whitespace-chars"
}
case class WhitespaceRatioFeatureExtractor() extends SingleFeatureExtractor {
  override def getFeatureName() = WhitespaceRatioFeatureExtractor.getFeatureName

  override def computeFeature(attribute: PreprocessedAttribute): Double = {
    val numRegex = """\s""".r
    val ratios = attribute.rawAttribute.values.map {
      case s if s.length > 0 => numRegex.findAllIn(s).size.toDouble / s.length
      case _ => 0.0
    }
    ratios.sum / ratios.size
  }

  override def computeSimpleFeature(attribute: SimpleAttribute): Double = {
    val numRegex = """\s""".r
    val ratios = attribute.values.map {
      case s if s.nonEmpty => numRegex.findAllIn(s).size.toDouble / s.length
      case _ => 0.0
    }
    ratios.sum / ratios.length
  }
}


object DiscreteTypeFeatureExtractor {
  def getFeatureName(): String = "is-discrete"
}
case class DiscreteTypeFeatureExtractor() extends SingleFeatureExtractor {
  override def getFeatureName() = DiscreteTypeFeatureExtractor.getFeatureName
  override def computeFeature(attribute: PreprocessedAttribute): Double = {
    if(attribute.preprocessedDataMap("is-discrete").asInstanceOf[Boolean]) 1.0 else 0.0
  }

  override def computeSimpleFeature(attribute: SimpleAttribute): Double = {
    val values = attribute.values.filter(_.trim.nonEmpty)
    val counts: Map[String,Int] = values
      .groupBy(identity)
      .map { case (k,v) => (k, v.length) }

    val total = counts.values.sum.toDouble
    // val avgInstances = total.toDouble / counts.size.toDouble
    // val isDiscrete = (avgInstances >= 0.05 * total)
    val isDiscrete: Boolean = (counts.size.toDouble / total) <= 0.3 //number of unique values is lte 30% of rowcount

    if(isDiscrete) 1.0 else 0.0
  }
}


/**
  * Information Theoretic features
  */
object EntropyForDiscreteDataFeatureExtractor {
  def getFeatureName(): String = "entropy-for-discrete-values"
}
case class EntropyForDiscreteDataFeatureExtractor() extends SingleFeatureExtractor {
  override def getFeatureName() = EntropyForDiscreteDataFeatureExtractor.getFeatureName
  override def computeFeature(attribute: PreprocessedAttribute): Double = {
    attribute.preprocessedDataMap("entropy").asInstanceOf[Double]
  }

  override def computeSimpleFeature(attribute: SimpleAttribute): Double = {
    val values = attribute.values.filter(_.trim.nonEmpty)
    val counts: Map[String,Int] = values
      .groupBy(identity)
      .map { case (k,v) => (k, v.length) }

    val total = counts.values.sum.toDouble
    val isDiscrete: Boolean = (counts.size.toDouble / total) <= 0.3 //number of unique values is lte 30% of rowcount

    if (isDiscrete) {
      counts.map {
        case (k, v) =>
          val prob = v.toDouble / total
          prob * Math.log(prob)
      }.sum * -1
    } else {
      -1.0
    }
  }
}


/**
  * Information Theoretic features
  * Shannon's entropy
  */
object ShannonEntropyFeatureExtractor {
  def getFeatureName(): String = "shannon-entropy"
}
case class ShannonEntropyFeatureExtractor() extends SingleFeatureExtractor {
  lazy val chars = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!\"#$%&\\'()*+,-./:;<=>?@[\\\\]^_`{|}~ \\t\\n\\r\\x0b\\x0c"

  // logarithm base 2
  lazy val lnOf2 = scala.math.log(2) // natural log of 2
  def log2(x: Double): Double = scala.math.log(x) / lnOf2

  override def getFeatureName() = ShannonEntropyFeatureExtractor.getFeatureName

  /**
    * add the information measure (negative entropy) of text,
    * Sum(p(i) log(p(i)), i=1,...,n), where p(i) is the i-th character frequency,
    * n is the number of possible characters in the "alphabet", i.e., number of possible states
    * @param normalisedCharDists normalized character frequency vector
    * @return
    */
  private def calculateShannonEntropy(normalisedCharDists: Map[Char,Double]): Double = {
    if (normalisedCharDists.isEmpty) {
      0.0
    } else {
      val max_entropy = - log2(1.0 / chars.length)
      val entropy = chars.toList.map {
        c => normalisedCharDists.getOrElse(c, 0.0)
      }.filter(_ > 0).map(c => c * log2(c)).sum
      // normalize entropy
      (- 1.0 * entropy) / max_entropy
    }
  }

  override def computeFeature(attribute: PreprocessedAttribute): Double = {
    val attrContent: Seq[String] = attribute.rawAttribute.values.filter(_.nonEmpty)
    if(attrContent.nonEmpty) {
      val normalisedCharDists: Map[Char,Double] = attribute
        .preprocessedDataMap.getOrElse("normalised-char-frequency-vector", Map())
        .asInstanceOf[Map[Char,Double]]

      calculateShannonEntropy(normalisedCharDists)
    } else {
      0.0
    }
  }

  override def computeSimpleFeature(attribute: SimpleAttribute): Double = {
    val normalisedCharDists: Map[Char,Double] = attribute.charDist
    calculateShannonEntropy(normalisedCharDists)
  }


}


object DatePatternFeatureExtractor {
  def getFeatureName(): String = "prop-datepattern"
}
case class DatePatternFeatureExtractor() extends SingleFeatureExtractor {
  val maxSampleSize = 100

  val datePattern1 = """^[0-9]+/[0-9]+/[0-9]+$""".r
  val datePattern2 = """^[a-zA-Z]+ [0-9]+, [0-9]+$""".r
  val datePattern3 = """[0-9]+:[0-9]+:[0-9]+$""".r
  val datePattern4 = """[0-9]+:[0-9]+$""".r

  override def getFeatureName() = DatePatternFeatureExtractor.getFeatureName

  override def computeFeature(attribute: PreprocessedAttribute): Double = {
    val rawAttr = attribute.rawAttribute
    if(rawAttr.values.isEmpty) {
      0.0
    } else {
      val numSample = if(rawAttr.values.size > maxSampleSize) maxSampleSize else rawAttr.values.size
      val randIdx = (new Random(124213)).shuffle(rawAttr.values.indices.toList).take(numSample)
      val regexResults = randIdx.filter {
        idx => datePattern1.pattern.matcher(rawAttr.values(idx)).matches ||
          datePattern2.pattern.matcher(rawAttr.values(idx)).matches ||
          datePattern3.pattern.matcher(rawAttr.values(idx)).matches ||
          datePattern4.pattern.matcher(rawAttr.values(idx)).matches
      }
      regexResults.size.toDouble / randIdx.size
    }
  }

  override def computeSimpleFeature(attribute: SimpleAttribute): Double = {
    if(attribute.values.isEmpty) {
      0.0
    } else {
      val numSample = if(attribute.values.length > maxSampleSize) maxSampleSize else attribute.values.length
      val randIdx = (new Random(124213)).shuffle(attribute.values.indices.toList).take(numSample)
      val regexResults = randIdx.filter {
        idx => datePattern1.pattern.matcher(attribute.values(idx)).matches ||
          datePattern2.pattern.matcher(attribute.values(idx)).matches ||
          datePattern3.pattern.matcher(attribute.values(idx)).matches ||
          datePattern4.pattern.matcher(attribute.values(idx)).matches
      }
      regexResults.size.toDouble / randIdx.size
    }
  }
}


object CharDistFeatureExtractor {
  def chars = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ!\"#$%&\\'()*+,-./:;<=>?@[\\\\]^_`{|}~ \\t\\n\\r\\x0b\\x0c"
  def getGroupName() = "char-dist-features"
  def getFeatureNames() = (1 to chars.length).map({"char-dist-" + _}).toList
}
case class CharDistFeatureExtractor() extends GroupFeatureExtractor {

  override def getGroupName(): String =
    CharDistFeatureExtractor.getGroupName

  override def getFeatureNames(): List[String] =
    CharDistFeatureExtractor.getFeatureNames

  override def computeSimpleFeatures(attribute: SimpleAttribute): List[Double] = {
    val normalisedCharDists = attribute.charDist
    if (normalisedCharDists.isEmpty) {
      CharDistFeatureExtractor.chars.map { _ => 0.0}.toList
    } else {
      CharDistFeatureExtractor.chars.toList.map {
        c => normalisedCharDists.getOrElse(c, 0.0)
      }
    }
  }

  override def computeFeatures(attribute: PreprocessedAttribute): List[Double] = {
    val attrContent: Seq[String] = attribute.rawAttribute.values.filter(_.nonEmpty)
    if(attrContent.nonEmpty) {
      val normalisedCharDists = attribute
        .preprocessedDataMap.getOrElse("normalised-char-frequency-vector", Map())
        .asInstanceOf[Map[Char,Double]]

      // this calculation leads to the fact that only chars present in the last row are in the distribution
//      val summedCharDists = attrContent.foldLeft[Map[Char,Double]](Map.empty[Char,Double])(
//        (props: Map[Char,Double], nextVal: String) => {
//          val charDist = computeNormalizedCharFreq(nextVal)
//          val newMap: Map[Char,Double] = charDist.keys.map {
//            k: Char =>
//              k -> (props.getOrElse(k,0.0) + charDist(k))
//          }.toMap
//          newMap
//        }
//      )
//      val normalisedCharDists = normalise(summedCharDists)

      CharDistFeatureExtractor.chars.toList.map {
        c => normalisedCharDists.getOrElse(c, 0.0)
      }
    } else {
      CharDistFeatureExtractor.chars.map { _ => 0.0}.toList
    }
  }

  def computeNormalizedCharFreq(s: String): Map[Char,Double] = {
//    val counts = scala.collection.mutable.Map[Char, Double]()
//    s.toCharArray.foreach({case c =>
//      counts += (c -> (counts.getOrElse(c, 0.0) + 1.0))
//    })
    // calculate frequencies of chars in the attribute values (rows of the column)
    val counts: Map[Char,Double] = s
      .toCharArray
      .groupBy(_.toChar)
      .mapValues(_.length.toDouble)

    if(counts.nonEmpty) {
      normalise(counts)
    } else {
      Map()
    }
  }

  def normalise(counts: Map[Char,Double]): Map[Char,Double] = {
    val maxCount = counts.values.max
    val dscaled = counts.values
      .map(_ /maxCount) // normalize counts
      .foldLeft(0.0)(_ + Math.pow(_, 2)) // compute sum of squares

    val norm = Math.sqrt(dscaled) * maxCount // the norm of the counts
    val normCf = norm match {
      case 0 => counts.map { case (x, _) => (x,0.0) }
      case normbig =>
        counts.map { case (x, y) => (x, y.toDouble / normbig) }
    }

//    val length = Math.abs(Math.sqrt(normCf.values.map({x=>x*x}).sum) - 1.0)
//    assert((length - 1.0) <= 0.00005,
//      "length of char freq vector is " + length
//        + "\nnorm: " + norm
//        + "\ncounts: " + counts
//        + "\ncounts^2: " + (counts.values.map({case x => x*x})))
    normCf
  }
}


/**
  *  Data type indicator fields
  **/
object DataTypeFeatureExtractor {
  def getGroupName(): String = "inferred-data-type"
}
case class DataTypeFeatureExtractor(typeMapFile: Option[String] = None
                                   ) extends GroupFeatureExtractor with LazyLogging {
  val keys = List(
    ("inferred-type-float", "float"),
    ("inferred-type-integer", "integer"),
    ("inferred-type-long", "long"),
    ("inferred-type-boolean", "boolean"),
    ("inferred-type-date", "date"),
    ("inferred-type-time", "time"),
    ("inferred-type-datetime", "datetime"),
    ("inferred-type-string", "string")
  )

  val typeMap: Option[Map[String,String]] =
    typeMapFile.map { f =>
      val lines = Source.fromFile(f).getLines.toList.drop(1)
      lines.map {
        l =>
          val toks = l.split(",")
          (toks(0), toks(1))
      }.toMap
    }

  //print out type map
  logger.info(typeMap.map({
    "***Type Map Supplied:\n\t" + _.mkString("\n\t")
  }).getOrElse("***No Type Map Supplied."))


  override def getGroupName(): String = DataTypeFeatureExtractor.getGroupName

  override def getFeatureNames(): List[String] = keys.map(_._1)

  override def computeSimpleFeatures(attribute: SimpleAttribute): List[Double] = {
    typeMap.flatMap {
      m =>
        val predefinedType = m.get(attribute.attributeName)
        predefinedType.map {
          t =>
            keys.map {
              case (fname, typename) => if(typename.equalsIgnoreCase(t)) 1.0 else 0.0
            }
        }
    }.getOrElse(
      keys.map {
        case (featureName, typeName) =>
          if(attribute.inferredMap(featureName)) 1.0 else 0.0
      })
  }

  override def computeFeatures(attribute: PreprocessedAttribute): List[Double] = {
    typeMap.flatMap {
      m =>
      val predefinedType = m.get(attribute.rawAttribute.id)
      predefinedType.map {
        t =>
          keys.map {
            case (fname, typename) => if(typename.equalsIgnoreCase(t)) 1.0 else 0.0
          }
      }
    }.getOrElse(
      keys.map {
        case (featureName, typeName) =>
          if(attribute.preprocessedDataMap(featureName).asInstanceOf[Boolean]) 1.0 else 0.0
      }
    )
  }
}


/**
  *  String Length Statistics.
  **/
object TextStatsFeatureExtractor {
  def getGroupName(): String = "stats-of-text-length"
}
case class TextStatsFeatureExtractor() extends GroupFeatureExtractor {

  override def getGroupName(): String = TextStatsFeatureExtractor.getGroupName

  override def getFeatureNames(): List[String] =
    List("stringLengthMean","stringLengthMedian",
      "stringLengthMode","stringLengthMin","stringLengthMax")

  override def computeFeatures(attribute: PreprocessedAttribute): List[Double] = {
    attribute.preprocessedDataMap("string-length-stats").asInstanceOf[List[Double]]
  }

  override def computeSimpleFeatures(attribute: SimpleAttribute): List[Double] = {
    val lengths = attribute.values.map(_.length)
    val mean: Double = lengths.sum.toDouble / lengths.length.toDouble

    val sortedLengths = lengths.sorted
    val median: Double = if(sortedLengths.length > 1) {
      sortedLengths(Math.ceil(lengths.length.toDouble/2.0).toInt - 1)
    } else {
      -1
    }

    val mode: Double = if(lengths.nonEmpty) {
      lengths.groupBy(identity).maxBy(_._2.length)._1
    } else {
      -1
    }

    val max: Double = lengths.foldLeft(0)({case (mx,v) => {if(v > mx) v else mx}})
    val min: Double = lengths.foldLeft(max)({case (mn,v) => {if(v < mn) v else mn}})
    List(mean, median, mode, min, max)
  }
}

/**
  *  Numerical values statistics.
  **/
object NumberTypeStatsFeatureExtractor {
  def getGroupName(): String = "stats-of-numerical-type"
}
case class NumberTypeStatsFeatureExtractor() extends GroupFeatureExtractor {
  val floatRegex = """(^[+-]?[0-9]*\.[0-9]+)|(^[+-]?[0-9]+)""".r

  override def getGroupName(): String =
    NumberTypeStatsFeatureExtractor.getGroupName

  override def getFeatureNames(): List[String] =
    List("numTypeMean","numTypeMedian",
      "numTypeMode","numTypeMin","numTypeMax")

  override def computeSimpleFeatures(attribute: SimpleAttribute): List[Double] = {
    if(attribute.inferredMap("inferred-type-integer") ||
      attribute.inferredMap("inferred-type-float") ||
      attribute.inferredMap("inferred-type-long")) {
      val values = attribute.values
        .filter{
          x => x.nonEmpty && floatRegex.pattern.matcher(x).matches
        }
        .map(_.toDouble)
      val mean = values.sum / values.length.toDouble

      val sortedValues = values.sorted
      val median = sortedValues(Math.ceil(values.length.toDouble/2.0).toInt - 1)

      val mode = values.groupBy(identity).maxBy(_._2.length)._1

      val max = values.foldLeft(0.0)({case (mx,v) => if(v > mx) v else mx})
      val min = values.foldLeft(max)({case (mn,v) => if(v < mn) v else mn})
      List(mean, median, mode, min, max)
    } else {
      List(-1.0, -1.0, -1.0, -1.0, -1.0)
    }
  }

  override def computeFeatures(attribute: PreprocessedAttribute): List[Double] = {
    if(attribute.preprocessedDataMap("inferred-type-integer").asInstanceOf[Boolean] ||
      attribute.preprocessedDataMap("inferred-type-float").asInstanceOf[Boolean] ||
      attribute.preprocessedDataMap("inferred-type-long").asInstanceOf[Boolean]) {
      val values = attribute.rawAttribute.values
        .filter{
          x => x.nonEmpty && floatRegex.pattern.matcher(x).matches
        }
        .map(_.toDouble)
      val mean = values.sum / values.size.toDouble

      val sortedValues = values.sorted
      val median = sortedValues(Math.ceil(values.size.toDouble/2.0).toInt - 1)

      val mode = values.groupBy(identity).maxBy(_._2.size)._1

      val max = values.foldLeft(0.0)({case (mx,v) => {if(v > mx) v else mx}})
      val min = values.foldLeft(max)({case (mn,v) => {if(v < mn) v else mn}})
      List(mean,median,mode,min,max)
    } else {
      List(-1.0,-1.0,-1.0,-1.0,-1.0)
    }
  }
}
