# Serene Data Integration Platform

Serene is a data integration platform designed to provide semantic matching across heterogenous relational data stores.

### Prerequisites

You will need sbt to build and run the platform. On mac:
```
brew install sbt
```
on Debian linux
```
sudo apt-get install sbt
```
## Usage
Start the web server...
```
sbt run
```
The API can be used with the following commands...

## General
By default the server will run on localhost, port 8080. This can be changed in `src/main/resources/application.conf`. To check that the server is running, ensure that the following endpoints return valid JSON:
```
# check version
curl localhost:8080

# simple test
curl localhost:8080/v1.0
```

## Datasets
Datasets need to be uploaded to the server. Currently only CSVs are supported. A description can also be added to the dataset upload.
```
# Get a list of datasets...
curl localhost:8080/v1.0/dataset

# Post a new dataset...
curl -X POST -F 'file=@test.csv' -F 'description=This is a file' -F 'typeMap={"a":"int", "c":"string", "e":"int"}' localhost:8080/v1.0/dataset

# Show a single dataset
curl localhost:8080/v1.0/dataset/12341234

# Show a single dataset with custom sample size
curl localhost:8080/v1.0/dataset/12341234?samples=50

# Update a single dataset
curl -X POST -F 'description=This is a file' -F 'typeMap={"a":"int", "c":"string", "e":"float"}' localhost:8080/v1.0/dataset/12341234

# Delete a dataset
curl -X DELETE  localhost:8080/v1.0/dataset/12341234
```

## Schema Matcher Models
The model endpoint controls the parameters used for the Schema Matcher classifier. The Schema Matcher takes a list of `classes`, and attempts to assign them to the columns of a dataset. If a column is known, use `labelData` to indicate the class to the ColumnID in the dataset. The `features`, `modelType` and `resamplingStrategy` can be modified.
```
# List models
curl localhost:8080/v1.0/model

# Post model
curl -X POST \
  -H "Content-Type: application/json" \
  -d '{
    "description": "This is the description",
    "modelType": "randomForest",
    "classes": ["name", "address", "phone", "unknown"],
    "features": { "activeFeatures" : [
          "num-unique-vals",
          "prop-unique-vals",
          "prop-missing-vals",
          "ratio-alpha-chars",
          "prop-numerical-chars",
          "prop-whitespace-chars",
          "prop-entries-with-at-sign", 
          "prop-entries-with-hyphen",
          "prop-range-format",
          "is-discrete", 
          "entropy-for-discrete-values"
        ],
        "activeFeatureGroups" : [
          "inferred-data-type",
          "stats-of-text-length",
          "stats-of-numeric-type",
          "prop-instances-per-class-in-knearestneighbours",
          "mean-character-cosine-similarity-from-class-examples",
          "min-editdistance-from-class-examples",
          "min-wordnet-jcn-distance-from-class-examples",
          "min-wordnet-lin-distance-from-class-examples"
        ],
        "featureExtractorParams" : [
             {
              "name" : "prop-instances-per-class-in-knearestneighbours",
              "num-neighbours" : 3
             },
             {
              "name" : "min-editdistance-from-class-examples",
              "max-comparisons-per-class" : 20
             },
             {
              "name" : "min-wordnet-jcn-distance-from-class-examples",
              "max-comparisons-per-class" : 20
             },
             {
              "name" : "min-wordnet-lin-distance-from-class-examples",
              "max-comparisons-per-class" : 20
             }
           ]
        },
    "costMatrix": [[1,0,0], [0,1,0], [0,0,1]],
    "labelData" : {"1696954974" : "name", "66413956": "address"},
    "resamplingStrategy": "ResampleToMean"
    }' \
  localhost:8080/v1.0/model


# Show a single model
curl localhost:8080/v1.0/model/12341234

# Update model (all fields optional)
curl -X POST \
  -H "Content-Type: application/json" \
  -d '{
    "description": "This is the description",
    "modelType": "randomForest",
    "classes": ["name", "address", "phone", "unknown"],
    "features": { "activeFeatures" : [
          "num-unique-vals",
          "prop-unique-vals",
          "prop-missing-vals",
          "ratio-alpha-chars",
          "prop-numerical-chars",
          "prop-whitespace-chars",
          "prop-entries-with-at-sign", 
          "prop-entries-with-hyphen",
          "prop-range-format",
          "is-discrete", 
          "entropy-for-discrete-values"
        ],
        "activeFeatureGroups" : [
          "inferred-data-type",
          "stats-of-text-length",
          "stats-of-numeric-type",
          "prop-instances-per-class-in-knearestneighbours",
          "mean-character-cosine-similarity-from-class-examples",
          "min-editdistance-from-class-examples",
          "min-wordnet-jcn-distance-from-class-examples",
          "min-wordnet-lin-distance-from-class-examples"
        ],
        "featureExtractorParams" : [
             {
              "name" : "prop-instances-per-class-in-knearestneighbours",
              "num-neighbours" : 3
             },
             {
              "name" : "min-editdistance-from-class-examples",
              "max-comparisons-per-class" : 20
             },
             {
              "name" : "min-wordnet-jcn-distance-from-class-examples",
              "max-comparisons-per-class" : 20
             },
             {
              "name" : "min-wordnet-lin-distance-from-class-examples",
              "max-comparisons-per-class" : 20
             }
           ]
        },
    "costMatrix": [[1,0,0], [0,1,0], [0,0,1]],
    "labelData" : {"1696954974" : "name", "66413956": "address"},
    "resamplingStrategy": "ResampleToMean"
    }' \
  localhost:8080/v1.0/model


# Train model (async, use GET on model 98793874 to query state)
curl -X POST localhost:8080/v1.0/model/98793874/train

# Delete a model
curl -X DELETE  localhost:8080/v1.0/model/12341234

# Predict a specific dataset 12341234 using model. Returns prediction JSON object
curl -X POST localhost:8080/v1.0/model/98793874/predict/12341234

```
## Tests
```
sbt test
```

