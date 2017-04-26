# Serene Data Integration Platform

Serene is a data integration platform designed to provide semantic matching across heterogeneous relational data stores.

### Prerequisites

You will need sbt to build and run the platform. On mac:
```
brew install sbt
```
on Debian linux
```
sudo apt-get install sbt
```

## Installation
You can build the library with
```
bin/build
```
This should build the Serene server and place the final jar into the `jars` directory.

Alternatively to use sbt
```
sbt assembly
```

## Usage
To start the web server use
```
bin/server-start
```
The following cmd line options are available:
```
--storage-path <value>  Storage Path determines the directory in which to store all files and objects
--host <value>          Server host address (default 127.0.0.1)
--port <value>          Server port number (default 8080)
--help                  Prints this usage text
```

Alternatively to use sbt, you can run
```
sbt run
```
with arguments in quotes e.g.
```
sbt "run --port 8888"
```

Additional configuration is available in [application.conf](http://github.com/NICTA/serene/blob/modeller/core/src/main/resources/application.conf), specifically for the initialization of Spark. 

The API can be used with the following commands...

## General
By default the server will run on localhost, port 8080. This can be changed in `src/main/resources/application.conf`. To check that the server is running, ensure that the following endpoints return valid JSON:
```
# check version
curl localhost:8080

# simple test
curl localhost:8080/v1.0
```

WARNING: the server will not work properly if logging level is set to DEBUG!

## Datasets
Datasets need to be uploaded to the server. Currently only CSVs are supported. A description can also be added to the dataset upload.
In case a dataset does not have headers, special header line needs to be added to the CSV (otherwise such dataset will not be properly read in by serene):
the header line should be numbers starting from 0 to the number of columns -1.
```
# Get a list of datasets...
curl localhost:8080/v1.0/dataset

# Post a new dataset...
# Note that the max upload size is 2GB...
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
             }, {
              "name" : "min-editdistance-from-class-examples",
              "max-comparisons-per-class" : 3
             }, {
              "name" : "min-wordnet-jcn-distance-from-class-examples",
              "max-comparisons-per-class" : 3
             }, {
              "name" : "min-wordnet-lin-distance-from-class-examples",
              "max-comparisons-per-class" : 3
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
             }, {
              "name" : "min-editdistance-from-class-examples",
              "max-comparisons-per-class" : 3
             }, {
              "name" : "min-wordnet-jcn-distance-from-class-examples",
              "max-comparisons-per-class" : 3
             }, {
              "name" : "min-wordnet-lin-distance-from-class-examples",
              "max-comparisons-per-class" : 3
             }
           ]
        },
    "costMatrix": [[1,0,0], [0,1,0], [0,0,1]],
    "labelData" : {"1696954974" : "name", "66413956": "address"},
    "resamplingStrategy": "ResampleToMean"
    }' \
  localhost:8080/v1.0/model/98793874


# Train model (async, use GET on model 98793874 to query state)
curl -X POST localhost:8080/v1.0/model/98793874/train

# Delete a model
curl -X DELETE  localhost:8080/v1.0/model/12341234

# Predict a specific dataset 12341234 using model. Returns prediction JSON object
curl -X POST localhost:8080/v1.0/model/98793874/predict/12341234

```

To use the newly added bagging resampling strategy ("Bagging", "BaggingToMax", "BaggingToMean"), additional parameters can be indicated in model post resquest: numBags and bagSize.
Both parameters are integer, and if not specified, default value 100 will be used for both.
Example model post request to use bagging:
 ```
curl -X POST \
  -H "Content-Type: application/json" \
  -d '{
    "description": "This is the description",
    "modelType": "randomForest",
    "classes": ["name", "address", "phone", "unknown"],
    "features": { "activeFeatures" : [ "num-unique-vals", "prop-unique-vals", "prop-missing-vals" ],
        "activeFeatureGroups" : [ "stats-of-text-length", "prop-instances-per-class-in-knearestneighbours"],
        "featureExtractorParams" : [{"name" : "prop-instances-per-class-in-knearestneighbours","num-neighbours" : 5}]
        },
    "costMatrix": [[1,0,0], [0,1,0], [0,0,1]],
    "labelData" : {"1" : "name", "1817136897" : "unknown", "1498946589" : "name", "134383522" : "phone", "463734360" : "address"},
    "resamplingStrategy": "Bagging",
    "numBags": 10,
    "bagSize": 1000
    }' \
  localhost:8080/v1.0/model
```

Explanation of features and the list of available features can be found [here](https://github.com/NICTA/serene/blob/master/matcher/dirstruct/semantic_type_classifier/repo/docs/features.txt).
Resampling strategies are enumerated [here](https://github.com/NICTA/serene/blob/master/matcher/dirstruct/semantic_type_classifier/HOWTO). 
Currently only `randomForest` is supported as a modelType through Serene API.

## Semantic Modelling

Attribute ids in the source descriptions are really important since we rely on Karma code to perform semantic modelling. We have to make sure that they are unique across different data sources.

The labels (semantic types) are assumed to come in the format: className---propertyName.

The configuration for the semantic modeler is specified in [modeling.properties](http://github.com/NICTA/serene/blob/modeller/modeler/src/main/resources/modeling.properties).

### Semantic Source Descriptions
Semantic source descriptions provide information how exactly a particular dataset maps into a specified ontology. They include information both about the semantic types (i.e., classes/labels) for the columns as well as information about the relationships of these semantic types. All this information is encoded in the semantic model.
Before a semantic source description can be uploaded to the server, the associated datasets should be uploaded.
```
# Get a list of semantic source descriptions...
curl localhost:8080/v1.0/ssd

# Post a new SSD...
# Note that the max upload size is 2GB...
curl -X POST \
  -H "Content-Type: application/json" \
  -d '{
      "name": "serene-user-example-ssd",
      "ontology": [1],
      "semanticModel": {
        "nodes": [
            {
               "id": 0,
               "label": "State",
               "type": "ClassNode"
           },
           {
               "id": 1,
               "label": "City",
               "type": "ClassNode"
           }],
       "links": [
           {
               "id":     0,
               "source": 1,
               "target": 0,
               "label": "isPartOf",
               "type": "ObjectPropertyLink"
           }]
      },
      "mappings": [
       {
            "attribute": 1997319549,
            "node": 0
       },
       {
           "attribute": 1160349990,
           "node": 1
       }],
    }' \
         localhost:8080/v1.0/ssd

# Show a single ssd
curl localhost:8080/v1.0/ssd/12341234

# Update a single ssd

# Delete a ssd
curl -X DELETE  localhost:8080/v1.0/ssd/12341234
```

### Ontologies

Serene can handle only OWL ontologies.
```
# Get a list of ontologies...
curl localhost:8080/v1.0/owl

# Post a new ontology...
# Note that the max upload size is 2GB...
curl -X POST -F 'file=@test.owl' localhost:8080/v1.0/owl

# Show a single owl
curl localhost:8080/v1.0/owl/12341234

# Update a single owl

# Delete a owl
curl -X DELETE  localhost:8080/v1.0/owl/12341234
```

### Octopus
The octopus endpoint controls the parameters used for the Semantic Modeller of the Serene API. Octopus is the final model which performs both relational and ontological schema matching. 
```
# List octopi
curl localhost:8080/v1.0/model

# Post octopus
curl -X POST \
  -H "Content-Type: application/json" \
  -d '{
      "name": "hello",
      "description": "Testing octopus used for identifying phone numbers only.",
      "ssds": [1, 2, 3],
      "ontologies": [1, 2, 3],
      "modelingProps": "not implemented for now",
      "modelType": "randomForest",
      "features": ["isAlpha", "alphaRatio", "atSigns", ...],
      "resamplingStrategy": "ResampleToMean",
      "numBags": 10,
      "bagSize": 10
    }' \
         localhost:8080/v1.0/octopus
  
# Train octopus (async, includes training for the schema matcher model, use GET on octopus 98793874 to query state)
curl -X POST localhost:8080/v1.0/octopus/98793874/train

# Delete a single octopus
curl -X DELETE  localhost:8080/v1.0/octopus/12341234

# Suggest a list of semanctic models for a specific dataset 12341234 using octopus. Returns prediction JSON object
curl -X POST localhost:8080/v1.0/octopus/98793874/predict/12341234
```

### Evaluation
Compute three metrics to compare a predicted SSD against the correct one
```
curl -X POST \
  -H "Content-Type: application/json" \
  -d '{
       "predictedSsd": {
         "name": "businessInfo.csv",
         "ontologies": [1],
         "semanticModel": {
           "nodes": [***],
           "links": [***]
         },
         "mappings": [***]
       },
       "correctSsd": {
         "name": "businessInfo.csv",
         "ontologies": [1],
         "semanticModel": {
           "nodes": [***],
           "links": [***]
         },
         "mappings": [***]
       },
       "ignoreSemanticTypes": true,
       "ignoreColumnNodes": true
      }' \
  localhost:8080/v1.0/evaluate
```

## Tests
To run all tests:
```
sbt test
```
To run individual module tests, refer to the module name e.g.
```
sbt serene-core/test
sbt serene-matcher/test
sbt serene-modeler/test
```
To run an individual test spec refer to the Spec e.g.
```
sbt "serene-core/test-only au.csiro.data61.core.SSDStorageSpec"
```

To generate the code coverage report:

```
sbt serene-core/test serene-core/coverageReport
```

This will generate an HTML report at ```core/target/scala-2.11/scoverage-report/index.html```


## Notes

For the semantic modelling part 3 [Karma](http://github.com/usc-isi-i2/Web-Karma) java libraries need to be available:
- karma-common;
- karma-typer;
- karma-util.

Certain changes have been made to the original Karma code:

1) Make the following methods public: SortableSemanticModel.steinerNodes.getSizeReduction.

2) Add method ModelLearningGraph.setLastUpdateTime:
```
public void setLastUpdateTime(long newTime) {
		this.lastUpdateTime = newTime;
	}
```

3) Add `DINT` to Karma origin of semantic types:
```
public enum Origin {
		AutoModel, User, CRFModel, TfIdfModel, RFModel, DINT
	}
```

4) Add two more parameters to the method in GraphBuilder.java:
```
private void updateLinkCountMap(DefaultLink link, Node source, Node target)
```
