package au.csiro.data61.matcher.data

import au.csiro.data61.matcher.data.LabelTypes._

package object LabelTypes {
    type EquivalentSet = Set[String]
    type CrossProdMatch = (Set[String],Set[String])
    type SemanticTypeLabelsMap = Map[String, String]
}

abstract class Labels {
    def ++(labels: Labels): Labels
}

trait ContainsPositiveLabels {
    def positiveLabels: List[EquivalentSet]
}

trait ContainsAmbiguousLabels {
    def ambiguousLabels: List[CrossProdMatch]
}

case class BasicLabels(val positiveLabels: List[EquivalentSet]) extends Labels with ContainsPositiveLabels {
    def ++(labels: Labels): Labels = labels match {
        case l: BasicLabels => BasicLabels(positiveLabels ++ l.positiveLabels)
        case l: PosAndAmbigLabels => PosAndAmbigLabels(positiveLabels ++ l.positiveLabels, l.ambiguousLabels)
    }
}

case class PosAndAmbigLabels(val positiveLabels: List[EquivalentSet], 
                             val ambiguousLabels: List[CrossProdMatch]) extends Labels with ContainsPositiveLabels 
                                                                                       with ContainsAmbiguousLabels {
    def ++(labels: Labels): Labels = labels match {
        case l: BasicLabels => PosAndAmbigLabels(positiveLabels ++ l.positiveLabels, ambiguousLabels)
        case l: PosAndAmbigLabels => PosAndAmbigLabels(positiveLabels ++ l.positiveLabels, ambiguousLabels ++ l.ambiguousLabels)
    }
}

// case class SemanticTypeLabels(val semanticTypes: SemanticTypeLabelsMap) extends Labels {
//     def ++(labels: Labels): Labels = labels match {
//         case l: SemanticTypeLabels => SemanticTypeLabels(semanticTypes ++ l.semanticTypes)
//     }
// }

case class SemanticTypeLabels(val labelsMap: Map[String, SemanticTypeLabel]) extends Labels {
    def ++(labels: Labels): Labels = labels match {
        case l: SemanticTypeLabels => SemanticTypeLabels(labelsMap ++ l.labelsMap)
    }

    def findLabel(attributeId: String): String = {
        labelsMap.get(attributeId).map({
            case l: ManualSemanticTypeLabel => l.actualClass
            case l: PredictedSemanticTypeLabel =>
                if(l.actualClass == "?") "unknown"
                else l.actualClass
        }).getOrElse("unknown")
    }
}

abstract class SemanticTypeLabel {
    def getGtClass: String
    def getId: String
}
case class ManualSemanticTypeLabel(val attributeId: String, val actualClass: String) extends SemanticTypeLabel {
    override def getGtClass(): String = actualClass
    override def getId(): String = attributeId
}
case class PredictedSemanticTypeLabel(
    val attributeId: String, 
    val predictedClass: String,
    val confidence: Double,
    val datePredicted: String,
    val actualClass: String,
    val dateValidated: String)  extends SemanticTypeLabel {

    override def getGtClass(): String = actualClass
    override def getId(): String = attributeId
}



