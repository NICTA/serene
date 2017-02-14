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

import org.json4s.native.JsonMethods.parse
import au.csiro.data61.ingest.JsonSchema.{from, merge, toJsonAst}

class JsonSchemaSpec extends UnitSpec {
  private val json = parse("""
    [
      {
        "num": 1.7,
        "optStr": "foo",
        "nullable": 1,
        "heterotype": [
          { "bool": true },
          [ 1, 2 ],
          [ "bar1", "bar2" ],
          [ { "nil": null }, null ]
        ]
      },
      {
        "num": 2.3,
        "nullable": null,
        "heterotype": { "str": "bar" }
      }
    ]
  """)

  "A JsonSchema" can "be extracted from a JSON value with depth > 1" in {
    val schema = parse("""
      {
        "type": "array",
        "elementSchema": {
          "type": "object",
          "objectSchema": {
            "optStr": { "type": "string", "optional": true },
            "heterotype": [
              { "type": "object", "objectSchema": { "str": { "type": "string" } } },
              {
                "type": "array",
                "elementSchema": [
                  { "type": "object", "objectSchema": { "bool": { "type": "boolean" } } },
                  {
                    "type": "array",
                    "elementSchema": [
                      { "type": "object", "objectSchema": { "nil": { "type": "null" } } },
                      { "type": "null" },
                      { "type": "string" },
                      { "type": "number" }
                    ]
                  }
                ]
              }
            ],
            "num": { "type": "number" },
            "nullable": [
              { "type": "null" },
              { "type": "number" }
            ]
          }
        }
      }
    """)

    assert(JsonSchema.toJsonAst(from(json)) == schema)
  }

  it can "be merged with another JsonSchema" in {
    val json2 = parse("""
        {
          "num": 2.96,
          "matrix": [
            [1, 2, 3],
            [4, 5, 6],
            [7, 8, 9]
          ]
        }
      """)

    val expectedSchema = parse("""
      [
        {
          "type": "array",
          "elementSchema": {
            "type": "object",
            "objectSchema": {
              "optStr": { "type": "string", "optional": true },
              "heterotype": [
                { "type": "object", "objectSchema": { "str": { "type": "string" } } },
                {
                  "type": "array",
                  "elementSchema": [
                    { "type": "object", "objectSchema": { "bool": { "type": "boolean" } } },
                    {
                      "type": "array",
                      "elementSchema": [
                        { "type": "object", "objectSchema": { "nil": { "type": "null" } } },
                        { "type": "null" },
                        { "type": "string" },
                        { "type": "number" }
                      ]
                    }
                  ]
                }
              ],
              "num": { "type": "number" },
              "nullable": [
                { "type": "null" },
                { "type": "number" }
              ]
            }
          }
        },
        {
          "type": "object",
          "objectSchema": {
            "num": { "type": "number" },
            "matrix": {
              "type": "array",
              "elementSchema": {
                "type": "array",
                "elementSchema": {
                  "type": "number"
                }
              }
            }
          }
        }
      ]
      """)

    assert(toJsonAst(merge(from(json), from(json2))) == expectedSchema)
  }
}
