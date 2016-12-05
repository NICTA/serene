package au.csiro.data61.matcher.matcher.train

import au.csiro.data61.matcher.data._
import au.csiro.data61.matcher.matcher._

import scala.util.Random
import com.typesafe.scalalogging.LazyLogging

object ClassImbalanceResampler extends LazyLogging {
    val validResamplingStrategies = Set(
        "UpsampleToMax",
        "ResampleToMean", "UpsampleToMean",
        "ResampleToMedian", "UpsampleToMedian",
        "CapUnknownToHalf",
        "NoResampling"
    )

    // seeds are fixed in all these functions

    def resampleToMax(attributes: List[Attribute],
                      labels: SemanticTypeLabels
                     ): List[Attribute] = {
        val instCountPerClass = attributes
          .map { case attr =>
            (labels.findLabel(attr.id), attr)}
          .groupBy(_._1)
          .map { case (classname, instances) =>
            (classname, instances, instances.size)}
          .toList
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
          .map {
              case attr =>
                  (labels.findLabel(attr.id), attr)
          }
          .groupBy(_._1)
          .map {
              case (classname, instances) =>
                  (classname, instances, instances.size)
          }
          .toList
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
          .map { case attr =>
            (labels.findLabel(attr.id), attr)}
          .groupBy(_._1)
          .map { case (classname, instances) =>
            (classname, instances, instances.size)}
          .toList
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
        if(allowDownSampling) resample(median, instCountPerClass, labels)
        else upsample(median, instCountPerClass, labels)
    }

    def resample(sampleSize: Int,
                 instCountPerClass: List[(String, List[(String, Attribute)], Int)],
                 labels: SemanticTypeLabels
                ): List[Attribute] = {

        val randGenerator = new scala.util.Random(5123219)
        instCountPerClass.flatMap { case (className: String, instances: List[(String,Attribute)], count: Int) =>
            if(count < sampleSize) {
                //upsample
                logger.info(s"***Upsampling...")
                val numSamplesToTake = sampleSize - count
                val newSamples = (0 until numSamplesToTake).map({case idx =>
                    val randIdx = randGenerator.nextInt(count)
                    instances(randIdx)._2
                }).toList
                instances.map({_._2}) ++ newSamples
            } else
            if(count > sampleSize) {
                //downsample
                logger.info(s"***Downsampling...")
                val numExcess = count - sampleSize
                (0 until numExcess).foldLeft(instances)((instances,idx) => {
                    val randIdx = randGenerator.nextInt(instances.size)
                    if(randIdx == 0) instances.drop(1)
                    else if(randIdx == (instances.size-1)) instances.dropRight(1)
                    else instances.take(randIdx) ++ instances.drop(randIdx+1)
                }).map({_._2})
            } else {
                instances.map({_._2})
            }
        }
    }

    def upsample(sampleSize: Int,
                 instCountPerClass: List[(String, List[(String, Attribute)], Int)],
                 labels: SemanticTypeLabels
                ): List[Attribute] = {
        val randGenerator = new scala.util.Random(8172310)
        instCountPerClass.flatMap { case (className: String, instances: List[(String,Attribute)], count: Int) =>
            if(count < sampleSize) {
                val numSamplesToTake = sampleSize - count
                val newSamples = (0 until numSamplesToTake).map({case idx =>
                    val randIdx = randGenerator.nextInt(count)
                    instances(randIdx)._2
                }).toList
                instances.map(_._2) ++ newSamples
            } else {
                instances.map(_._2)
            }
        }
    }

    def resample(strategy: String,
                 attributes: List[Attribute],
                 labels: SemanticTypeLabels
                ): List[Attribute] = {
        logger.info("***Resampling instances with: " + strategy)
//        logger.info("   attributes obtained: "+attributes.map(_.id).mkString(","))
//        logger.info("   semantictypelabels")
//        logger.info(s"${labels}")

        val resampledAttrs = strategy match {
            case "UpsampleToMax" => resampleToMax(attributes, labels)
            case "ResampleToMean" => resampleToMean(attributes, labels, true)
            case "UpsampleToMean" => resampleToMean(attributes, labels, false)
            case "ResampleToMedian" => resampleToMedian(attributes, labels, true)
            case "UpsampleToMedian" => resampleToMedian(attributes, labels, false)
            case "CapUnknownToHalf" => {
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
            case "CostMatrix" => attributes // TODO: not available in Spark MlLib currently
            case "NoResampling" => attributes

            case x => {
                //Invalid sampling method.
                logger.warn("!!!! ERROR: Invalid sampling strategy: " + x + " !!!!")
                List()
            }
        }

        //shuffle
        (new Random(10857171)).shuffle(resampledAttrs)
    }
}
