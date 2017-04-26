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
package au.csiro.data61.core.storage

import java.io._
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

import au.csiro.data61.types._
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.FileUtils
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

/**
  * The Readable trait is for a Key T that can be read from a string
  *
  * @tparam T The Key type
  */
trait Readable[T] {
  def read(x: String): T
}

/**
  * Companion object containing helper functions and standard implementations
  */
object Readable {

  /**
    * Helper function which allows creation of Readable instances
    */
  def toReadable[T](p: String => T): Readable[T] = new Readable[T] {
    def read(x: String): T = p(x)
  }

  /**
    * Allow for construction of standalone readables, if the ops aren't used
    */
  def apply[A](implicit instance: Readable[A]): Readable[A] = instance

  /**
    * Functions for implicits
    */
  implicit val ReadableDouble = toReadable[Double](_.toDouble)
  implicit val ReadableInt = toReadable[Int](_.toInt)
  implicit val ReadableOptionInt = toReadable[Option[Int]](p => Some(p.toInt))
  implicit val ReadableLong = toReadable[Long](_.toLong)
  implicit val ReadableString = toReadable[String](new String(_))
  implicit val ReadableBoolean = toReadable[Boolean](_.toBoolean)
  implicit val ReadableCharList = toReadable[List[Char]](_.toCharArray.toList)
  implicit val ReadableStringList = toReadable[List[String]](_.split(':').toList)

}


/**
 * Storage object that can store objects that are identifiable by a `Key`.
 * The child storage objects need to specify the root directory and an
 * `extract` method to convert a string to a `Value`.
 *
 * This maintains a cache map of the current state, and manages the hard
 * disk layer, storing objects in json format.
 *
 */
trait Storage[Key, Value <: Identifiable[Key]] extends LazyLogging with JsonFormats {

  implicit val keyReader: Readable[Key]

  protected def rootDir: String

  protected def extract(stream: FileInputStream): Value

  lazy val cache = collection.mutable.Map(listValues.map(m => m.id -> m).toSeq: _*)

  def keys: List[Key] = {
    cache.keys.toList
  }

  def add(id: Key, value: Value): Option[Key] = {
    logger.info(s"Adding $id to storage")

    Try {
      synchronized {
        writeToFile(value)
        cache += (id -> value)
      }
      id
    } match {
      case Success(key) =>
        Some(key)
      case Failure(err) =>
        logger.error(s"Failed to write the resource file to disk: ${err.getMessage}")
        None
    }
  }

  /**
   * Returns the model object at location id
   *
   * @param id The key for the model
   * @return Resource if available
   */
  def get(id: Key): Option[Value] = {
    cache.get(id)
  }

  /**
   * Attempts to read all the objects out from the storage dir
   *
   * @return
   */
  protected def listValues: List[Value] = {
    listDirectories(rootDir)
      .flatMap(toKeyOption)
      .map(getPath)
      .flatMap(readFromFile)
  }

  /**
   * Helper function to list the directories
   *
   * @param rootDir The root directory from which to search
   * @return
   */
  protected def listDirectories(rootDir: String): List[String] = {
    Option(new File(rootDir) listFiles) match {
      case Some(fileList) =>
        fileList
          .filter(_.isDirectory)
          .map(_.getName)
          .toList
      case _ =>
        logger.warn(s"Attempting to list directories. Failed to open dir $rootDir")
        List.empty[String]
    }
  }

  protected def toKeyOption(s: String): Option[Key] = {
    Try(keyReader.read(s)).toOption
  }

  /**
   * Returns the location of the JSON metadata file for id
   *
   * @param id The ID for the Value
   * @return
   */
  protected def getPath(id: Key): Path = {
    Paths.get(getDirectoryPath(id).toString, s"$id.json")
  }

  /**
    * Returns the location of the directory for id
    *
    * @param id The ID for the Value
    * @return
    */
  protected def getDirectoryPath(id: Key): Path = {
    Paths.get(rootDir, s"$id")
  }

  /**
   * Attempts to read a JSON file and convert it into a Value
   * using the JSON reader.
   *
   * @param path Location of the JSON metadata file
   * @return
   */
  protected def readFromFile(path: Path): Option[Value] = {
    Try {
      val stream = new FileInputStream(path.toFile)
      // needs to use the pre-defined extract method...
      extract(stream)
    } match {
      case Success(value) =>
        Some(value)
      case Failure(err) =>
        logger.error(s"Failed to read file: ${err.getMessage}")
        None
    }
  }

  /**
   * Update the model id in the storage layer
   *
   * @param id ID to give to the element
   * @param value Value object
   * @return ID of the resource created (if any)
   */
  def update(id: Key, value: Value, deleteRF : Boolean = true): Option[Key] = {
    add(id, value)
  }

  /**
   * Writes the object to disk as a serialized json string
   * at a pre-defined location based on the id.
   *
   * @param value The value to write to disk
   */
  protected def writeToFile(value: Value): Unit = {
    val str = compact(Extraction.decompose(value))

    val outputPath = getPath(value.id)

    // ensure that the directories exist...
    val dir = outputPath.toFile.getParentFile

    if (!dir.exists) dir.mkdirs

    // write the object to the file system
    Files.write(
      outputPath,
      str.getBytes(StandardCharsets.UTF_8)
    )
  }

  /**
    * Deletes the resource at the key `id`. This will also
    * delete any file resource at this key
    *
    * @param id Key for the value to be removed
    * @return Key of the removed value if successful
    */
  def remove(id: Key): Option[Key] = {
    cache.get(id) match {
      case Some(ds) =>

        // delete directory - be careful
        val dir: File = getPath(id).getParent.toFile

        synchronized {
          Try(FileUtils.deleteDirectory(dir)) match {
            case Failure(err) =>
              logger.error(s"Failed to delete directory: ${err.getMessage}")
              None
            case _ =>
              cache -= id
              Some(id)
          }
        }
      case _ =>
        logger.error(s"Resource not found: $id")
        None
    }
  }

  def removeAll(): Unit = {
    keys.foreach(remove)
  }

}


