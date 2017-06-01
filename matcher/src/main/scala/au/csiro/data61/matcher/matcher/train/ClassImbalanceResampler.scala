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

import breeze.linalg.min
import au.csiro.data61.matcher.data._
import au.csiro.data61.matcher.matcher._

import scala.util.Random
import language.postfixOps
import com.typesafe.scalalogging.LazyLogging

object ClassImbalanceResampler extends LazyLogging {
    val validResamplingStrategies = Set(
        "UpsampleToMax",
        "ResampleToMean", "UpsampleToMean",
        "ResampleToMedian", "UpsampleToMedian",
        "CapUnknownToHalf",
        "NoResampling",
        "Bagging",
        "BaggingToMax",
        "BaggingToMean"
    )

    // seeds are fixed in all these functions

    def resampleToMax(attributes: List[Attribute],
                      labels: SemanticTypeLabels
                     ): List[Attribute] = {
        val instCountPerClass = attributes
          .map {
              attr => (labels.findLabel(attr.id), attr)
          }
          .groupBy(_._1)
          .map {
              case (classname, instances) =>
                  (classname, instances, instances.size)
          } toList
        val maxClass = instCountPerClass.maxBy(_._3)
        val maxCount = maxClass._3
        logger.info(s"***Resampling all class instances to the maximum count of $maxCount.")
        resample(maxCount, instCountPerClass, labels)
    }

    def resampleToMean(attributes: List[Attribute],
                       labels: SemanticTypeLabels,
                       allowDownSampling: Boolean
                      ): List[Attribute] = {
        val instCountPerClass = attributes
          .map { attr => (labels.findLabel(attr.id), attr) }
          .groupBy(_._1)
          .map {
              case (classname, instances) =>
                  (classname, instances, instances.size)
          } toList
        val mean = instCountPerClass
          .map(_._3)
          .sum.toDouble / instCountPerClass.size.toDouble
        if(allowDownSampling) {
            logger.info(s"***Downsampling all class instances to the mean of ${mean.toInt}.")
            resample(mean.toInt, instCountPerClass, labels)
        }
        else {
            logger.info(s"***Upsampling all class instances to the mean of ${mean.toInt}.")
            upsample(mean.toInt, instCountPerClass, labels)
        }
    }

    def resampleToMedian(attributes: List[Attribute],
                         labels: SemanticTypeLabels,
                         allowDownSampling: Boolean
                        ): List[Attribute] = {
        val instCountPerClass = attributes
          .map { attr => (labels.findLabel(attr.id), attr)}
          .groupBy(_._1)
          .map {
              case (classname, instances) =>
                  (classname, instances, instances.size)
          } toList
        val median = instCountPerClass
          .sortBy(_._3)
          .map(_._3) match {
            case x => {
                val count = x.size
                if(count % 2 != 0) {
                    x(count/2)
                } else {
                    (x((count/2)-1) + x(count/2))/2
                }
            }
        }

        logger.info(s"***Resampling all class instances to the median of ${median}.")
        if(allowDownSampling) {
          resample(median, instCountPerClass, labels)
        } else {
          upsample(median, instCountPerClass, labels)
        }
    }

    def resample(sampleSize: Int,
                 instCountPerClass: List[(String, List[(String, Attribute)], Int)],
                 labels: SemanticTypeLabels
                ): List[Attribute] = {

        val randGenerator = new scala.util.Random(5123219)
        instCountPerClass.flatMap {
            case (className: String, instances: List[(String,Attribute)], count: Int) =>
                if(count < sampleSize) {
                    //upsample
                    logger.debug(s"***Upsampling...")
                    val numSamplesToTake = sampleSize - count
                    val newSamples = (0 until numSamplesToTake).map{
                      idx =>
                        val randIdx = randGenerator.nextInt(count)
                        instances(randIdx)._2
                    }.toList
                  //FIXME: attributes will not have unique attribute ids any more
                    instances.map(_._2) ++ newSamples
                } else
                if(count > sampleSize) {
                    //downsample
                    logger.debug(s"***Downsampling...")
                    val numExcess = count - sampleSize
                    (0 until numExcess).foldLeft(instances)((instances,idx) => {
                        val randIdx = randGenerator.nextInt(instances.size)
                        if(randIdx == 0) instances.drop(1)
                        else if(randIdx == (instances.size-1)) instances.dropRight(1)
                        else instances.take(randIdx) ++ instances.drop(randIdx+1)
                    }).map(_._2)
                } else {
                    instances.map(_._2)
                }
        }
    }

    def upsample(sampleSize: Int,
                 instCountPerClass: List[(String, List[(String, Attribute)], Int)],
                 labels: SemanticTypeLabels
                ): List[Attribute] = {
        val randGenerator = new scala.util.Random(8172310)
        instCountPerClass
          .flatMap {
              case (className: String, instances: List[(String,Attribute)], count: Int) =>
                if(count < sampleSize) {
                    val numSamplesToTake = sampleSize - count
                    val newSamples = (0 until numSamplesToTake).map {
                        idx =>
                            val randIdx = randGenerator.nextInt(count)
                            instances(randIdx)._2
                    } toList

                  //FIXME: attributes will not have unique attribute ids any more
                    instances.map(_._2) ++ newSamples
                } else {
                    instances.map(_._2)
                }
          }
    }

    /**
      * Helper method to perform bagging on a single attribute.
      * It creates numBags bags for the attribute
      * where each bag contains bagSize number of rows randomly picked from the values of the attribute.
      * @param attribute Column
      * @param bagSize Number of rows to be picked from the attribute per each bag.
      * @param numBags Number of bags to be generated.
      * @return
      * @throws Exception if attribute does not have enough values for bag sampling.
      */
    private def bagSampleAttribute(attribute: Attribute,
                                   bagSize: Int,
                                   numBags: Int): List[Attribute] = {
        if(attribute.values.size < bagSize){
            throw new Exception("Attribute does not have enough values for sampling the bag.")
        }

        //NOTE: attributes will not have unique attribute ids any more
        if (attribute.values.size > bagSize) {
            // we need to randomly pick bagSize rows into each bag
            (1 to numBags).map {
                idx =>
                    val randGenerator = new scala.util.Random(min(501 * idx, Int.MaxValue -1))
                    val sampleVals = randGenerator.shuffle(attribute.values).take(bagSize)
                    new Attribute(attribute.id, attribute.metadata, sampleVals, attribute.parent)
            } toList
        } else {
            List.fill(numBags)(attribute) // we replicate attribute numBags times
        }
    }

    /**
      * Helper method to perform bagging.
      * Calculates a list of numBags to be generated per each attribute in the same class group.
      * It's a helper method for class imbalance resampling.
      * @param sampleSize Number of samples to be produced in total per class group.
      * @param instancesSize Number of  available attributes in the class group.
      * @return
      */
    private def numBagsSequence(sampleSize: Int,
                                instancesSize: Int): List[Int] = {

        var bagsGenerated = 0
        (0 until instancesSize).map{
            idx =>
                val instSize = instancesSize - idx
                val curBags = (sampleSize - bagsGenerated) / instSize
                bagsGenerated += curBags
                curBags
        } toList
    }

    /**
      * Use bagging + resampling strategy.
      * @param sampleSize Number of samples to be produced in total per class group.
      * @param instCountPerClass Number of attributes per class before resampling
      * @param bagSize Number of rows to be randomly sampled into a bag
      * @param numBags Number of bags to be produced per column
      * @return
      */
    private def baggingResample(sampleSize: Int,
                        instCountPerClass: List[(String, List[(String, Attribute)], Int)],
                        bagSize: Int,
                        numBags: Int): List[Attribute] = {
        instCountPerClass.flatMap {
            case (className: String, instances: List[(String, Attribute)], count: Int) =>
                val newSampleAttrs: List[Attribute] =
                    if(sampleSize/instances.size < 1){
                        // we have to downsample attributes since we have a way more of them than required bags
                        val randGenerator = new scala.util.Random(min(count, Int.MaxValue -1))
                        val selectedAttrs = randGenerator.shuffle(instances.map(_._2)).take(sampleSize)
                        selectedAttrs.flatMap {
                            attr => bagSampleAttribute(attr, bagSize, 1) // we create 1 bag per attribute
                        }
                    } else {
                        val numBagsSeq = numBagsSequence(sampleSize, instances.size)
                        instances.map(_._2).zip(numBagsSeq).flatMap {
                            case (attr, genNumBags) => bagSampleAttribute(attr, bagSize, genNumBags)
                        }
                    }
            newSampleAttrs
        }
    }

    /**
      * Method to sample attributes by using bagging.
      * Here, we tackle two problems: class imbalance and insufficient data.
      * A bag is a collection of rows randomly picked from a column.
      * This is used for prediction!
      * @param attributes Original columns.
      * @param bagSize Number of rows to be randomly picked from the column into a bag, default 100.
      * @param numBags Number of bags to be generated per column to tackle the issue of insufficient data,
      *                   default 100.
      * @return Sampled feature vectors to be used for prediction.
      */
    def testBagging(attributes: List[Attribute],
                    bagSize: Int = DefaultBagging.BagSize,
                    numBags: Int = DefaultBagging.NumBags
                   ): List[Attribute] = {
        logger.debug("***Performing bagging...")
        attributes
          .flatMap {
              attr => if (attr.values.size < bagSize) {
                  bagSampleAttribute(attr, bagSize, numBags)
              } else {
                  // we then will do sampling with replacement
                  logger.debug("Row size is not sufficient for indicated bagSize. " +
                    "Sampling with replacement will be used.")
                  val replValues = (1 to 1 + bagSize / attr.values.size)
                    .foldLeft(attr.values){(cur,v) => cur ::: attr.values}
                  val newAttr = new Attribute(attr.id, attr.metadata, replValues, attr.parent)
                  bagSampleAttribute(newAttr, bagSize, numBags)
              }
          }
    }

    /**
      * Method to sample one attribute by using bagging.
      * Here, we tackle two problems: class imbalance and insufficient data.
      * A bag is a collection of rows randomly picked from a column.
      * This is used for prediction!
      * @param attr Original column.
      * @param bagSize Number of rows to be randomly picked from the column into a bag, default 100.
      * @param numBags Number of bags to be generated per column to tackle the issue of insufficient data,
      *                   default 100.
      * @return Sampled feature vectors to be used for prediction.
      */
    def testBaggingAttribute(attr: Attribute,
                    bagSize: Int = DefaultBagging.BagSize,
                    numBags: Int = DefaultBagging.NumBags
                   ): List[Attribute] = {
//        logger.debug("***Performing bagging...")
        if (attr.values.size > bagSize) {
            bagSampleAttribute(attr, bagSize, numBags)
        } else {
            // we then will do sampling with replacement
            logger.debug("Row size is not sufficient for indicated bagSize. " +
              "Sampling with replacement will be used.")
            val replValues = (1 to 1 + bagSize / attr.values.size)
              .foldLeft(attr.values){(cur,v) => cur ::: attr.values}
            val newAttr = new Attribute(attr.id, attr.metadata, replValues, attr.parent)
            bagSampleAttribute(newAttr, bagSize, numBags)
        }
    }

    /**
      * Method to sample attributes by using bagging.
      * Here, we tackle two problems: class imbalance and insufficient data.
      * A bag is a collection of rows randomly picked from a column.
      * @param strategy Resampling strategy to be used to tackle class imbalance.
      * @param attributes Original columns.
      * @param labels Semantic labels/types for columns.
      * @param bagSize Number of rows to be randomly picked from the column into a bag, default 100.
      * @param numBags Number of bags to be generated per column to tackle the issue of insufficient data,
      *                   default 100.
      * @return Sampled feature vectors to be used in training.
      */
    def bagging(strategy: String,
                attributes: List[Attribute],
                labels: SemanticTypeLabels,
                bagSize: Int = 100,
                numBags: Int = 100): List[Attribute] = {
        logger.info("***Performing bagging...")
        val instCountPerClass = attributes
          .map {
              attr =>
                  val bagSizeAttr: Attribute = if (attr.values.size > bagSize) {
                      attr
                  } else {
                      logger.debug("Row size is not sufficient for indicated bagSize. " +
                        "Sampling with replacement will be used.")
                      // we then will do sampling with replacement
                      val replValues = (1 to 1 + bagSize / attr.values.size)
                        .foldLeft(attr.values){(cur,v) => cur ::: attr.values}
                      new Attribute(attr.id, attr.metadata, replValues, attr.parent)
                  }
                  (labels.findLabel(attr.id), bagSizeAttr)
          }
          .groupBy(_._1)
          .map {
              case (classname, instances) =>
                  (classname, instances, numBags * instances.size) // the amount of bags generated per each class
          } toList

        val baggedAttrs: List[Attribute] = strategy match {
            case "None" =>
                instCountPerClass.flatMap {
                    case (className: String, instances: List[(String, Attribute)], count: Int) =>
                        instances.flatMap {
                            case (cl: String, attr: Attribute) => bagSampleAttribute(attr, bagSize, numBags)
                        }
                }
            case "ResampleToMean" =>
                val sampleSize = (instCountPerClass.map(_._3).sum.toDouble / instCountPerClass.size.toDouble).toInt
                logger.info(s"Bagging with strategy $strategy to sample size $sampleSize")
                baggingResample(sampleSize, instCountPerClass, bagSize, numBags)
            case "ResampleToMax" =>
                val sampleSize = instCountPerClass.maxBy(_._3)._3
                logger.info(s"Bagging with strategy $strategy to sample size $sampleSize")
                baggingResample(sampleSize, instCountPerClass, bagSize, numBags)
        }

        baggedAttrs
    }

    private def capUnknown(attributes: List[Attribute], labels: SemanticTypeLabels): List[Attribute] = {
        val (knownSet, unknownSet) = attributes.partition({a => labels.findLabel(a.id) != "unknown"})
        val maxUnknownSetSize = Math.min(knownSet.size, unknownSet.size)
        if(unknownSet.size > maxUnknownSetSize) {
            //subsample "unknown" classes to reduce bias
            val subsampledUnknownSet = (new Random(716241)).shuffle(unknownSet).take(maxUnknownSetSize)
            logger.warn("   subsampled 'unknown' instances from " + unknownSet.size + " to " + subsampledUnknownSet.size)
            knownSet ::: subsampledUnknownSet
        } else {
            attributes
        }
    }

    def resample(strategy: String,
                 attributes: List[Attribute],
                 labels: SemanticTypeLabels,
                 bagSize: Int = 100,
                 numBags: Int = 100
                ): List[Attribute] = {
        logger.info("***Resampling instances with: " + strategy)

        val resampledAttrs = strategy match {
            case "UpsampleToMax" => resampleToMax(attributes, labels)
            case "ResampleToMean" => resampleToMean(attributes, labels, true)
            case "UpsampleToMean" => resampleToMean(attributes, labels, false)
            case "ResampleToMedian" => resampleToMedian(attributes, labels, true)
            case "UpsampleToMedian" => resampleToMedian(attributes, labels, false)
            case "CapUnknownToHalf" => capUnknown(attributes, labels)
            case "CostMatrix" =>
                logger.info("Cost matrix is not available. No resampling is done.")
                attributes // TODO: not available in Spark MlLib currently
            case "NoResampling" => attributes
            case "Bagging" => bagging("None", attributes, labels, bagSize, numBags)
            case "BaggingToMax" => bagging("ResampleToMax", attributes, labels, bagSize, numBags)
            case "BaggingToMean" => bagging("ResampleToMean", attributes, labels, bagSize, numBags)

            case x =>
                logger.warn("!!!! ERROR: Invalid sampling strategy: " + x + " !!!!")
                List()
        }
        //shuffle
        (new Random(10857171)).shuffle(resampledAttrs)
    }
}
