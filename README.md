# Schema Matcher REST API

Simple REST API for the data-integration project. Uses Finagle + Finch as the interface to the data-integration code.

## Usage
Start the web server...
```
sbt run
```
The API can be used with the following commands...
```
# simple test
curl localhost:8080/v1.0

# Get a list of datasets...
curl localhost:8080/v1.0/dataset

# Post a new dataset...
curl -X POST -F 'file=@test.csv' -F 'description=This is a file' -F 'typeMap={"a":"b", "c":"d", "e":"f"}' localhost:8080/v1.0/dataset

# List a single dataset
curl localhost:8080/v1.0/dataset/12341234

# Update a single dataset
curl -X POST -F 'description=This is a file' -F 'typeMap={"a":"b", "c":"d", "e":"f"}' localhost:8080/v1.0/dataset/12341234

# Delete a dataset
curl -X DELETE  localhost:8080/v1.0/dataset/12341234

# List models
curl localhost:8080/v1.0/model

# Post model
curl -X POST \
  -H "Content-Type: application/json" \
  -d '{
    "description": "This is the description",
    "modelType": "randomForest",
    "labels": ["name", "address", "phone"],
    "features": { "activeFeatures" : [ "num-unique-vals", "prop-unique-vals", "prop-missing-vals" ],
        "activeFeatureGroups" : [ "stats-of-text-length", "prop-instances-per-class-in-knearestneighbours"],
        "featureExtractorParams" : [{"name" : "prop-instances-per-class-in-knearestneighbours","num-neighbours" : 5}]
        },
    "costMatrix": [[1,0,0], [0,1,0], [0,0,1]],
    "userData" : {"1" : "name", "1470086804" : "name"},
    "resamplingStrategy": "ResampleToMean"
    }' \
  localhost:8080/v1.0/model

# Show a single model
curl localhost:8080/v1.0/model/12341234

# Train a single model
curl localhost:8080/v1.0/model/12341234/train

```
## Tests
```
sbt test
```
