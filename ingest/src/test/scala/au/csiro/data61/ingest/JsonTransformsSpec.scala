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
package au.csiro.data61.ingest

import au.csiro.data61.ingest.JsonTransforms.{flatten, flattenMax, toCsv}
import org.json4s.{JArray, JObject}
import org.json4s.native.JsonMethods.parse

class JsonTransformsSpec extends UnitSpec {
  "Applying flatten to a JSON value" can "transform it to similar 1-level-flatter objects" in {
    val result = JArray(flatten(parse("""
      {
        "name": "Jon Snow",
        "weapons": ["sword", "bow"]
        "friends": [{"name": "Bran Stark"}, {"name": "Samwell Tarly"}],
        "father": {"name": "Ned Stark"}
        "???": []
      }
    """)).toList)

    val expected = parse("""
        [{
          "name":"Jon Snow",
          "father.name":"Ned Stark",
          "weapons":"sword",
          "friends":{"name":"Bran Stark"}
        },
        {
          "name":"Jon Snow",
          "father.name":"Ned Stark",
          "weapons":"sword",
          "friends":{"name":"Samwell Tarly"}
        },
        {
          "name":"Jon Snow",
          "father.name":"Ned Stark",
          "weapons":"bow",
          "friends":{"name":"Bran Stark"}
        },
        {
          "name":"Jon Snow",
          "father.name":"Ned Stark",
          "weapons":"bow",
          "friends":{
            "name":"Samwell Tarly"
          }
        }]
      """)

    assert(result == expected)
  }

  "Applying flattenMax to a JSON value" can "transform it to similar flat objects" in {
    val result = JArray(flattenMax(parse("""
      {
        "name": "Jon Snow",
        "weapons": ["sword", "bow"]
        "friends": [
          {"name": "Bran Stark", "weapons": ["magic"]},
          {"name": "Samwell Tarly", "weapons": ["sword", "tactics"]}
        ],
        "father": {"name": "Ned Stark"}
      }
    """)).toList)

    val expected = parse("""
        [{
          "name":"Jon Snow",
          "father.name":"Ned Stark",
          "weapons":"sword",
          "friends.name":"Bran Stark",
          "friends.weapons":"magic"
        },{
          "name":"Jon Snow",
          "father.name":"Ned Stark",
          "weapons":"sword",
          "friends.name":"Samwell Tarly",
          "friends.weapons":"sword"
        },{
          "name":"Jon Snow",
          "father.name":"Ned Stark",
          "weapons":"sword",
          "friends.name":"Samwell Tarly",
          "friends.weapons":"tactics"
        },{
          "name":"Jon Snow",
          "father.name":"Ned Stark",
          "weapons":"bow",
          "friends.name":"Bran Stark",
          "friends.weapons":"magic"
        },{
          "name":"Jon Snow",
          "father.name":"Ned Stark",
          "weapons":"bow",
          "friends.name":"Samwell Tarly",
          "friends.weapons":"sword"
        },{
          "name":"Jon Snow",
          "father.name":"Ned Stark",
          "weapons":"bow",
          "friends.name":"Samwell Tarly",
          "friends.weapons":"tactics"
        }]
      """)

    assert(result == expected)
  }

  "Applying toCsv to some JSON objects" can "transform them into CSV keys and values" in {
    val jsonObjects = Seq(
      parse("""
        {"name": "Jon Snow", "weapons": ["sword", "bow"]}
      """).asInstanceOf[JObject],
      parse("""
        {"name": "Ned Stark"}
      """).asInstanceOf[JObject]
    )

    val csv = (
      Seq("name", "weapons"),
      Seq(
        Seq("Jon Snow", """["sword","bow"]"""),
        Seq("Ned Stark", "")
      ),
      Seq.empty
    )

    assert(toCsv(jsonObjects) == csv)
  }
}
