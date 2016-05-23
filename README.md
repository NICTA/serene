# Schema Matcher REST API

## Features

* Scala 2.11
* Scalatra 2.2.0
* SBT IDEA 1.3.0

## Usage

    # Start the web server...
    sbt "container:start"

Datasets can be changed with

    curl localhost:8080/v1.0
    curl localhost:8080/v1.0/dataset
    curl -X POST -F 'file=@test.csv' -F 'description=This is a file' -F 'typeMap={"a":"b", "c":"d", "e":"f"}' localhost:8080/v1.0/dataset
    curl localhost:8080/v1.0/dataset/<id_number_returned>

## Tests

    ./sbt test
