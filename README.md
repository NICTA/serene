# Serene: Graph Analytics

Serene is an analytics platform for building, analyzing and visualizing graphs. The software provides a set of machine learning algorithms for graphs to be used for social network analysis, exploring network data, and running analytics across connected, heterogenous datasets. The software also contains a user interface for converting multiple datastores into a graph form, and conversely can be streamed to a relational store from a graph.

Supported data stores include:
 * sql (jdbc)
 * csv
 * json
 * neo4j

The algorithms available include:

* Machine Learning
    * Node Classification
    * Attribute Inference
    * Link Prediction
    * Community Detection
    * Pattern Recognition
    * Influence Propagation

* Graph Statistics
    * Centrality
    * Degree
    * Closeness
    * Local clustering
    * Eigenvector decomposition
    * Hits
    * Neighbor connectivity
    * Vertex embeddedness

* Node Embedding
    * Node2Vec
    * MetaPath
    * Similarity Measure


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
