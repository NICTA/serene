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
import au.csiro.data61.ingest.JsonSchema.from

class JsonSchemaSpec extends UnitSpec {
  "A JsonSchema" can "be extracted from a JSON value with depth > 1" in {
    val json = parse("""
      {
        "name": "Jon Snow",
        "weapons": ["sword", "bow"]
        "friends": [
          {"name": "Bran Stark", "weapons": ["magic"]},
          {"name": "Samwell Tarly", "weapons": "sword"},
          {"name": 123, "valid": false}
        ],
        "father": {"name": "Ned Stark"},
        "matrix": [
          ["one", "two", "three"],
          [1, 2, 3]
        ]
      }
    """)

    val schema = parse("""
      {
        "name": {"type": "string"},
        "weapons": {"type": "array", "schema": "string"},
        "friends": {
          "type": "array",
          "schema": {
            "valid": {
              "type": "boolean",
              "optional": true
            },
            "weapons": {
              "type": ["string", "array"],
              "optional": true
            },
            "name": {
              "type": ["number", "string"]
            }
          }
        },
        "father": {
          "type": "object",
          "schema": {
            "name": {"type": "string"}
          }
        },
        "matrix": {"type": "array"}
      }
    """)

    assert(from(json).toJsonAst == schema)
  }
}
