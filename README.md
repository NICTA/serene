# Schema Matcher REST API

Simple REST API for the data-integration project. Uses Scalatra as the interface to the data-integration code.

## Usage
Start the web server...
```
sbt "container:start"
```
Server can be talked to with the following commands...
```
curl localhost:8080/v1.0
curl localhost:8080/v1.0/dataset
curl -X POST -F 'file=@test.csv' -F 'description=This is a file' -F 'typeMap={"a":"b", "c":"d", "e":"f"}' localhost:8080/v1.0/dataset
curl localhost:8080/v1.0/dataset/<id_number_returned>
```
## Tests
```
./sbt test
```
