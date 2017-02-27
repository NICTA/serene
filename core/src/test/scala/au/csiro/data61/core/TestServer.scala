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

import java.net.InetSocketAddress
import com.twitter.finagle.builder.ClientBuilder
import com.twitter.finagle.{http, Http}
import com.twitter.finagle.http.Response
import com.twitter.util.{Closable, Await}

/**
 * This launches a test server on localhost...
 */
class TestServer {

  // This is used to pass itself to tests implicitly
  implicit val s = this

  val server = Serene.defaultServer
  val client = Http.newService(Serene.serverAddress)

  val JsonHeader = "application/json"

  def fullUrl(path: String): String = s"http://${Serene.serverAddress}$path"

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
   * Close the server and client after each test
   */
  def assertClose(): Unit = {
    Closable.all(server, client).close()
  }
}
