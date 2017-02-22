package au.csiro.data61.modeler

import java.io.File

import au.csiro.data61.matcher.types.{DataSet, MatcherJsonFormats}
import com.twitter.finagle.{Http, http}
import com.twitter.finagle.http.{FileElement, RequestBuilder, Response}
import com.twitter.io.Reader
import com.twitter.util.Await
import com.typesafe.scalalogging.LazyLogging
import org.json4s.jackson.JsonMethods._

import scala.util.Try

/**
  * Created by natalia on 13/10/16.
  */
class TestMatcherClient extends LazyLogging with MatcherJsonFormats {
  implicit val formats = json4sFormats

  val client = Http.newService(Config.MatcherAddress)

  val JsonHeader = "application/json;charset=utf-8"

  def fullUrl(path: String): String = s"http://${Config.MatcherAddress}$path"

  /**
    * Helper function to build a get request
    *
    * @param path URL path of the resource
    * @return Http response object
    */
  def get(path: String): Response = {
    val request = http.Request(http.Method.Get, path)
    Await.result(client(request))
  }

  /**
    * Helper function to build a delete request
    *
    * @param path URL of the endpoint to delete
    * @return
    */
  def delete(path: String): Response = {
    val request = http.Request(http.Method.Delete, path)
    Await.result(client(request))
  }

  /**
    * Posts a request to build a dataset, then returns the DataSet object it created
    * wrapped in a Try.
    *
    * @param file The location of the csv resource
    * @param typeMap The json string of the string->string typemap
    * @param description Description line to add to the file.
    * @return DataSet that was constructed
    */
  def postAndReturn(file: String, typeMap: String, description: String): Try[DataSet] = {

    Try {
      val content = Await.result(Reader.readAll(Reader.fromFile(new File(file))))

      val request = RequestBuilder().url(fullUrl(s"/${Config.APIVersion}/dataset"))
        .addFormElement("description" -> description)
        .addFormElement("typeMap" -> typeMap)
        .add(FileElement("file", content, None, Some(typeMap)))
        .buildFormPost(multipart = true)

      val response = Await.result(client(request))

      parse(response.contentString).extract[DataSet]
    }
  }
}
