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
package au.csiro.data61.core

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging

import scala.util.{Try, Success, Failure}

/**
 * This class holds the options for the command line user args
 */
case class ConfigArgs(storagePath: Option[String] = None,
                      serverHost: Option[String] = None,
                      serverPort: Option[Int] = None)

/**
  * This object loads in the configuration .conf
  * file and parses the values into fields.
  */
case class Config(storagePath: String,
                  datasetStorageDir: String,
                  modelStorageDir: String,
                  serverHost: String,
                  serverPort: Int,
                  numWorkers: Option[Int],
                  parallelFeatureExtraction: Boolean) extends LazyLogging

object Config extends LazyLogging {

  protected val parser = new scopt.OptionParser[ConfigArgs]("server-start") {
    head("Serene", "0.1.0")

    opt[String]("storage-path") action { (x, c) =>
      c.copy(storagePath = Some(x))
    } text "Storage Path determines the directory in which to store all files and objects"

    opt[String]("host") action { (x, c) =>
      c.copy(serverHost = Some(x))
    } text "Server host address (default 127.0.0.1)"

    opt[Int]("port") action { (x, c) =>
      c.copy(serverPort = Some(x))
    } text "Server port number (default 8080)"

    help("help") text "Prints this usage text"
  }

  protected def buildArgs(args: Array[String]): ConfigArgs = {
    // parser.parse returns Option[C]
    parser.parse(args, ConfigArgs()) map { config =>
      config
    } getOrElse {
      logger.error("Failed to parse arguments")
      // arguments are bad, usage message will have been displayed
      throw new Exception("Failed to parse arguments.")
    }
  }

  protected def processSparkNumWorkers(conf: com.typesafe.config.Config
                                      ): Option[Int] = {
    Try {
      conf.getInt("config.spark-num-workers")
    } match {
      case Success(0) =>
        logger.warn(s"Setting number of workers to default.")
        None
      case Success(num) =>
        logger.info(s"Setting number of workers to default.")
        Some(num)
      case Failure(err) =>
        logger.warn(s"Spark number of workers not properly indicated in config: ${err.getMessage}.")
        logger.warn(s"Setting number of workers to default.")
        None
    }
  }

  protected def processParallelFeatureExtraction(conf: com.typesafe.config.Config
                                      ): Boolean = {
    Try {
      conf.getBoolean("config.spark-feature-extraction")
    } match {
      case Success(fe) =>
        logger.info(s"Setting parallel feature extraction to $fe")
        fe
      case Failure(err) =>
        logger.warn(s"Parallel feature extraction not properly indicated in config: ${err.getMessage}.")
        logger.info(s"Setting parallel feature extraction to false")
        false
    }
  }

  /**
    * Constructor from main application args. Here the arguments are parsed,
    * and if not present, are replaced with the default from application.conf
    *
    * @param args
    * @return
    */
  def apply(args: Array[String],
            spark_conf: Option[(Boolean,Int)] = None
           ): Config = {

    val conf: com.typesafe.config.Config = ConfigFactory.load()

    val userArgs = buildArgs(args)

    // first we grab the defaults...
    val defaultStoragePath = conf.getString("config.output-dir")
    val defaultServerHost = conf.getString("config.server-host")
    val defaultServerPort = conf.getString("config.server-port")

    // spark args are available only in file config for now..
    val (numWorkers, parallelFeatureExtraction) = spark_conf match {
      case Some((parallel: Boolean, num: Int)) =>
        if (num < 1) {
          (None, parallel)
        } else {(Some(num), parallel)}
      case _ =>
        (processSparkNumWorkers(conf), processParallelFeatureExtraction(conf))
    }

    val storagePath = userArgs.storagePath.getOrElse(defaultStoragePath)
    val serverHost = userArgs.serverHost.getOrElse(defaultServerHost)
    val serverPort = userArgs.serverPort.getOrElse(defaultServerPort.toInt)

    // The model and dataset location are calculated differently, they
    // are subdirectories of the storage location and are not available
    // to the user...
    val DatasetDirName = conf.getString("config.output-dataset-dir")
    val ModelDirName = conf.getString("config.output-model-dir")

    val dataSetStorageDir = s"$storagePath/$DatasetDirName"
    val modelStorageDir = s"$storagePath/$ModelDirName"

    logger.info(s"Starting Server at host=$serverHost port=$serverPort")
    logger.info(s"Storage path at $storagePath")
    logger.info(s"Dataset repository at $dataSetStorageDir")
    logger.info(s"Model repository at $modelStorageDir")

    Config(
      storagePath = storagePath,
      datasetStorageDir = dataSetStorageDir,
      modelStorageDir = modelStorageDir,
      serverHost = serverHost,
      serverPort = serverPort,
      numWorkers = numWorkers,
      parallelFeatureExtraction = parallelFeatureExtraction
    )
  }
}
