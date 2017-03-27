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
package au.csiro.data61.matcher.ingestion.loader

import com.github.tototoshi.csv._
import scala.annotation.switch
import scala.io.Source
import java.io._
import java.util.NoSuchElementException
import com.typesafe.scalalogging.LazyLogging

/**
  * Re-defining totoshi csv reader
  * We need to change line terminator
  */
class TotoshiCsvReader protected (private val lineReader: LineReader)(implicit format: CSVFormat) extends CSVReader(lineReader)(format){
  val parser = new TotoshiCsvParser(format)

  override def readNext(): Option[List[String]] = {

    @scala.annotation.tailrec
    def parseNext(lineReader: LineReader, leftOver: Option[String] = None): Option[List[String]] = {

      val nextLine = lineReader.readLineWithTerminator()
      if (nextLine == null) {
        if (leftOver.isDefined) {
          throw new MalformedCSVException("Malformed Input!: " + leftOver)
        } else {
          None
        }
      } else {
        val line = leftOver.getOrElse("") + nextLine
        parser.parseLine(line) match {
          case None => {
            parseNext(lineReader, Some(line))
          }
          case result => result
        }
      }
    }

    parseNext(lineReader)
  }
}

object TotoshiCsvReader {

  val DEFAULT_ENCODING = "UTF-8"

  def open(source: Source)(implicit format: CSVFormat): TotoshiCsvReader = new TotoshiCsvReader(new SourceLineReader(source))(format)

  def open(reader: Reader)(implicit format: CSVFormat): TotoshiCsvReader = new TotoshiCsvReader(new ReaderLineReader(reader))(format)

  def open(file: File)(implicit format: CSVFormat): TotoshiCsvReader = {
    open(file, this.DEFAULT_ENCODING)(format)
  }

  def open(file: File, encoding: String)(implicit format: CSVFormat): TotoshiCsvReader = {
    val fin = new FileInputStream(file)
    try {
      open(new InputStreamReader(fin, encoding))(format)
    } catch {
      case e: UnsupportedEncodingException => fin.close(); throw e
    }
  }

  def open(filename: String)(implicit format: CSVFormat): TotoshiCsvReader =
    open(new File(filename), this.DEFAULT_ENCODING)(format)

  def open(filename: String, encoding: String)(implicit format: CSVFormat): TotoshiCsvReader =
    open(new File(filename), encoding)(format)

}

object TotoshiCsvParser {

  private type State = Int
  private final val Start = 0
  private final val Field = 1
  private final val Delimiter = 2
  private final val End = 3
  private final val QuoteStart = 4
  private final val QuoteEnd = 5
  private final val QuotedField = 6

  /**
    * {{{
    * scala> com.github.tototoshi.csv.CSVParser.parse("a,b,c", '\\', ',', '"')
    * res0: Option[List[String]] = Some(List(a, b, c))
    *
    * scala> com.github.tototoshi.csv.CSVParser.parse("\"a\",\"b\",\"c\"", '\\', ',', '"')
    * res1: Option[List[String]] = Some(List(a, b, c))
    * }}}
    */
  def parse(input: String, escapeChar: Char, delimiter: Char, quoteChar: Char): Option[List[String]] = {
    println(s"====> input: $input")
    val buf: Array[Char] = input.toCharArray
    var fields: Vector[String] = Vector()
    var field = new StringBuilder
    var state: State = Start
    var pos = 0
    val buflen = buf.length

    if (buf.length > 0 && buf(0) == '\uFEFF') {
      pos += 1
    }

    while (state != End && pos < buflen) {
      val c = buf(pos)
      (state: @switch) match {
        case Start => {
          c match {
            case `quoteChar` => {
              state = QuoteStart
              pos += 1
            }
            case `delimiter` => {
              fields :+= field.toString
              field = new StringBuilder
              state = Delimiter
              pos += 1
            }
//            case '\n' | '\u2028' | '\u2029' | '\u0085' => {
            case '\n' => {
              fields :+= field.toString
              field = new StringBuilder
              state = End
              pos += 1
            }
            case '\r' => {
              if (pos + 1 < buflen && buf(1) == '\n') {
                pos += 1
              }
              fields :+= field.toString
              field = new StringBuilder
              state = End
              pos += 1
            }
            case x => {
              field += x
              state = Field
              pos += 1
            }
          }
        }
        case Delimiter => {
          c match {
            case `quoteChar` => {
              state = QuoteStart
              pos += 1
            }
            case `escapeChar` => {
              if (pos + 1 < buflen
                && (buf(pos + 1) == escapeChar || buf(pos + 1) == delimiter)) {
                field += buf(pos + 1)
                state = Field
                pos += 2
              } else {
                throw new MalformedCSVException(buf.mkString)
              }
            }
            case `delimiter` => {
              fields :+= field.toString
              field = new StringBuilder
              state = Delimiter
              pos += 1
            }
//            case '\n' | '\u2028' | '\u2029' | '\u0085' => {
            case '\n' => {
              fields :+= field.toString
              field = new StringBuilder
              state = End
              pos += 1
            }
            case '\r' => {
              if (pos + 1 < buflen && buf(1) == '\n') {
                pos += 1
              }
              fields :+= field.toString
              field = new StringBuilder
              state = End
              pos += 1
            }
            case x => {
              field += x
              state = Field
              pos += 1
            }
          }
        }
        case Field => {
          c match {
            case `escapeChar` => {
              if (pos + 1 < buflen) {
                if (buf(pos + 1) == escapeChar
                  || buf(pos + 1) == delimiter) {
                  field += buf(pos + 1)
                  state = Field
                  pos += 2
                } else {
                  throw new MalformedCSVException(buf.mkString)
                }
              } else {
                state = QuoteEnd
                pos += 1
              }
            }
            case `delimiter` => {
              fields :+= field.toString
              field = new StringBuilder
              state = Delimiter
              pos += 1
            }
//            case '\n' | '\u2028' | '\u2029' | '\u0085' => {
            case '\n' => {
              fields :+= field.toString
              field = new StringBuilder
              state = End
              pos += 1
            }
            case '\r' => {
              if (pos + 1 < buflen && buf(1) == '\n') {
                pos += 1
              }
              fields :+= field.toString
              field = new StringBuilder
              state = End
              pos += 1
            }
            case x => {
              field += x
              state = Field
              pos += 1
            }
          }
        }
        case QuoteStart => {
          c match {
            case `escapeChar` if escapeChar != quoteChar => {
              if (pos + 1 < buflen) {
                if (buf(pos + 1) == escapeChar
                  || buf(pos + 1) == quoteChar) {
                  field += buf(pos + 1)
                  state = QuotedField
                  pos += 2
                } else {
                  throw new MalformedCSVException(buf.mkString)
                }
              } else {
                throw new MalformedCSVException(buf.mkString)
              }
            }
            case `quoteChar` => {
              if (pos + 1 < buflen && buf(pos + 1) == quoteChar) {
                field += quoteChar
                state = QuotedField
                pos += 2
              } else {
                state = QuoteEnd
                pos += 1
              }
            }
            case x => {
              field += x
              state = QuotedField
              pos += 1
            }
          }
        }
        case QuoteEnd => {
          c match {
            case `delimiter` => {
              fields :+= field.toString
              field = new StringBuilder
              state = Delimiter
              pos += 1
            }
//            case '\n' | '\u2028' | '\u2029' | '\u0085' => {
            case '\n' => {
              fields :+= field.toString
              field = new StringBuilder
              state = End
              pos += 1
            }
            case '\r' => {
              if (pos + 1 < buflen && buf(1) == '\n') {
                pos += 1
              }
              fields :+= field.toString
              field = new StringBuilder
              state = End
              pos += 1
            }
            case _ => {
              throw new MalformedCSVException(buf.mkString)
            }
          }
        }
        case QuotedField => {
          c match {
            case `escapeChar` if escapeChar != quoteChar => {
              if (pos + 1 < buflen) {
                if (buf(pos + 1) == escapeChar
                  || buf(pos + 1) == quoteChar) {
                  field += buf(pos + 1)
                  state = QuotedField
                  pos += 2
                } else {
                  throw new MalformedCSVException(buf.mkString)
                }
              } else {
                throw new MalformedCSVException(buf.mkString)
              }
            }
            case `quoteChar` => {
              if (pos + 1 < buflen && buf(pos + 1) == quoteChar) {
                field += quoteChar
                state = QuotedField
                pos += 2
              } else {
                state = QuoteEnd
                pos += 1
              }
            }
            case x => {
              field += x
              state = QuotedField
              pos += 1
            }
          }
        }
        case End => {
          sys.error("unexpected error")
        }
      }
    }
    (state: @switch) match {
      case Delimiter => {
        fields :+= ""
        Some(fields.toList)
      }
      case QuotedField => {
        None
      }
      case _ => {
        if (!field.isEmpty) {
          // When no crlf at end of file
          state match {
            case Field | QuoteEnd => {
              fields :+= field.toString
            }
            case _ => {
            }
          }
        }
        Some(fields.toList)
      }
    }
  }
}

class TotoshiCsvParser(format: CSVFormat) {

  def parseLine(input: String): Option[List[String]] = {
    val parsedResult = TotoshiCsvParser.parse(input, format.escapeChar, format.delimiter, format.quoteChar)
    if (parsedResult == Some(List("")) && format.treatEmptyLineAsNil) Some(Nil)
    else parsedResult
  }

}