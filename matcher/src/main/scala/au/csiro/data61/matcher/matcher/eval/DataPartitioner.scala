package au.csiro.data61.matcher.matcher.eval

import au.csiro.data61.matcher.data._
import au.csiro.data61.matcher.data.LabelTypes._

import scala.util.Random
import scala.math._

case class DataPartitioner() {

    /**
     * This method partitions datasets by attributes.  That is, we take all attributes
     * across all datasets, then split them randomly.  However, we do want to sample
     * the same proportions of attributes from each dataset.
     */
    def partitionByAttribute(datasets: List[DataModel], labels: Labels, propTrain: Double, randNumGenerator: Random = new Random(1000)): 
                                 ((List[DataModel],Labels),(List[DataModel],Labels)) = {
        val posLabels = labels match {
            case l: ContainsPositiveLabels => l.positiveLabels
            case _ => List()
        }
        val ambiguousLabels = labels match {
            case l: ContainsAmbiguousLabels => l.ambiguousLabels
            case _ => List()
        }

        //partition labels into train/test. since labels are usually very few, we partition by index of
        //the shuffled labels list instead of sampling with random numbers.
        val numTrain = Math.floor(propTrain * posLabels.size).toInt
        val shuffledLabels = Random.shuffle(posLabels)
        val trainLabels = shuffledLabels.take(numTrain)
        val testLabels = shuffledLabels.takeRight(posLabels.size - numTrain)
        val trainLabelAttrs = trainLabels.flatMap({_.toList}).distinct
        val testLabelAttrs = testLabels.flatMap({_.toList}).distinct

        //partition the rest of the attributes including labels into train/test for all datasets.
        val allAttributesByDS = datasets.map({case ds => (ds.id, DataModel.getAllAttributes(ds))})
        val allPartitionedAttributes = allAttributesByDS.map({case (dsId, attrs) => {
            val (train,test) = attrs.partition({
                case a if (trainLabelAttrs contains a.id) => true  //is a training label
                case a if (testLabelAttrs contains a.id) => false  //is a test label
                case _ => randNumGenerator.nextFloat <= propTrain  //assign randomly
            })
            (dsId, (train, test))
        }}).toMap

        val partitionedDataSets = datasets.map({case ds => (DataModel.copy(ds,None,allPartitionedAttributes(ds.id)._1.map(_.id).toSet), 
                                                            DataModel.copy(ds,None,allPartitionedAttributes(ds.id)._2.map(_.id).toSet))
                                  })
        val allTrainingData = partitionedDataSets.map({_._1}).toList
        val allTestData = partitionedDataSets.map({_._2}).toList
        
        ((allTrainingData, PosAndAmbigLabels(trainLabels,ambiguousLabels)), (allTestData, PosAndAmbigLabels(testLabels,ambiguousLabels)))
    }

    /**
     * This method partitions datasets by their container/sources.
     */
    def partitionByDataSet(datasets: List[DataModel], labels: Labels, propTrain: Double, randNumGenerator: Random = new Random(1000)): 
                                 ((List[DataModel],Labels),(List[DataModel],Labels)) = {
        val posLabels = labels match {
            case l: ContainsPositiveLabels => l.positiveLabels
            case _ => List()
        }
        val ambiguousLabels = labels match {
            case l: ContainsAmbiguousLabels => l.ambiguousLabels
            case _ => List()
        }

        //partition datasets into train and test.
        val numTrain = Math.floor(propTrain * datasets.size).toInt
        val shuffledDataSets = Random.shuffle(datasets)
        val trainDataSets = shuffledDataSets.take(numTrain)
        val testDataSets = shuffledDataSets.takeRight(datasets.size - numTrain)

        //partition the labels.
        val allTrainSetAttrs = trainDataSets.flatMap(DataModel.getAllAttributes).map({_.id})
        val allTestSetAttrs = testDataSets.flatMap(DataModel.getAllAttributes).map({_.id})

        val trainLabels = posLabels.map({case s => s.filter({case l => allTrainSetAttrs contains l})}).filter(_.size > 1)
        val testLabels = posLabels.map({case s => s.filter({case l => allTestSetAttrs contains l})}).filter(_.size > 1)
        
        ((trainDataSets, PosAndAmbigLabels(trainLabels,ambiguousLabels)), (testDataSets, PosAndAmbigLabels(testLabels,ambiguousLabels)))
    }   
}