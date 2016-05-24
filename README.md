# Schema Matcher REST API

Simple REST API for the data-integration project. Uses Scalatra as the interface to the data-integration code.

## Usage
Start the web server...
```
sbt "container:start"
```
The API can be used with the following commands...
```
curl localhost:8080/v1.0
```
Get a list of datasets...
```
curl localhost:8080/v1.0/dataset
```
Post a new dataset...
```
curl -X POST -F 'file=@test.csv' -F 'description=This is a file' -F 'typeMap={"a":"b", "c":"d", "e":"f"}' localhost:8080/v1.0/dataset
```
List a single dataset
```
curl localhost:8080/v1.0/dataset/12341234
```
Update a single dataset
```
curl -X PATCH -F 'description=This is a file' -F 'typeMap={"a":"b", "c":"d", "e":"f"}' localhost:8080/v1.0/dataset/12341234
```
Delete a dataset
```
curl -X DELETE  localhost:8080/v1.0/dataset/12341234
```
## Tests
```
./sbt test
```
