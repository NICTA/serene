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
package au.csiro.data61.matcher.api

import au.csiro.data61.matcher.oauth.{OAuthUser, InMemoryDataHandler}
import com.twitter.finagle.oauth2.{AuthInfo, GrantHandlerResult}
import io.finch._
import io.finch.oauth2._
import org.json4s.jackson.JsonMethods._

/**
  * A simple example of finch-oauth2 usage
  *
  * Use the following sbt command to run the application.
  *
  * {{{
  *   $ sbt 'examples/runMain io.finch.oauth2.Main'
  * }}}
  *
  * Use the following HTTPie commands to test endpoints.
  *
  * {{{
  *   $ http POST :8081/users/auth Authorization:'OAuth dXNlcl9pZDp1c2VyX3NlY3JldA=='\
  *     grant_type==client_credentials
  *
  *   $ http POST :8081/users/auth grant_type==password username==user_name\
  *     password==user_password client_id==user_id
  *
  *   $ http POST :8081/users/auth grant_type==authorization_code code==user_auth_code client_id==user_id
  *
  *   $ http GET :8081/users/users/current access_token=='AT-5b0e7e3b-943f-479f-beab-7814814d0315'
  *
  *   $ http POST :8081/users/auth client_id==user_id grant_type==refresh_token\
  *     refresh_token=='RT-7e1bbf43-e7ba-4a8a-a38e-baf62ce3ceae'
  *
  *   $ http GET :8081/users/unprotected
  * }}}
  */

object OAuthRestAPI extends RestAPI {
  case class UnprotectedUser(name: String)

  val users: Endpoint[OAuthUser] = get(APIVersion :: "users" :: "current" :: authorize(InMemoryDataHandler)) {
    ai: AuthInfo[OAuthUser] =>
      Ok(ai.user)
  }

  val tokens: Endpoint[GrantHandlerResult] = post(APIVersion :: "users" :: "auth" :: issueAccessToken(InMemoryDataHandler))

  val unprotected: Endpoint[UnprotectedUser] = get(APIVersion :: "users" :: "unprotected") {
    Ok(UnprotectedUser("unprotected"))
  }

  val endpoints =
    tokens :+:
    users :+:
    unprotected
}
