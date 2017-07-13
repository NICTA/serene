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

val mainVersion = "0.2.0"

/**
  * Common Serene project settings for all projects...
  */
lazy val commonSettings = Defaults.coreDefaultSettings ++ Seq(

  // long file names become an issue on encrypted file systems - this is a weird workaround
  scalacOptions ++= Seq("-Xmax-classfile-name", "78"),

  scalaVersion := "2.11.8",

  libraryDependencies ++= Seq(
    "org.apache.spark"            %%  "spark-core"           % "2.1.0",
    "org.apache.spark"            %%  "spark-sql"            % "2.1.0",
    "org.apache.spark"            %%  "spark-mllib"          % "2.1.0",
    "org.apache.commons"          %   "commons-csv"          % "1.4",
    "joda-time"                   %   "joda-time"            % "2.9.9")
)


/**
  * Serene algorithm module. Holds the implementations of the algorithms for machine learning, social network analysis, and graph traversal.
  */
lazy val algorithm = Project(
  id = "serene-algorithm",
  base = file("algorithm")
)
  .settings(commonSettings)
  .settings(
    name := "serene-algorithm",
    organization := "au.csiro.data61.serene",
    version := mainVersion,

    libraryDependencies ++= Seq(
      "org.json4s"                  %% "json4s-jackson"     % "3.2.10"
      ,"org.json4s"                 %% "json4s-native"      % "3.2.10"
      ,"org.json4s"                 %% "json4s-ext"         % "3.2.10"
      ,"com.typesafe.scala-logging" %% "scala-logging"      % "3.4.0"
      ,"org.scalatest"              %% "scalatest"          % "3.0.0-RC1"
      ,"com.typesafe"               %  "config"             % "1.3.0"
      ,"org.scala-graph"            %% "graph-core"         % "1.11.2"         // scala library to work with graphs
      ,"org.jgrapht"                %  "jgrapht-core"       % "0.9.0"          // Karma uses java library to work with graphs
      ,"org.json"                   %  "json"               % "20141113"       // dependency for Karma
    )
  )


/**
  * Serene common module. Holds the global types, constants and data structures for the system.
  */
lazy val common = Project(
  id = "serene-common",
  base = file("common")
)
  .settings(commonSettings)
  .settings(
    name := "serene-common",
    organization := "au.csiro.data61.serene",
    version := mainVersion,

    libraryDependencies ++= Seq(
      "org.json4s"                  %% "json4s-jackson"     % "3.2.10"
      ,"org.json4s"                 %% "json4s-native"      % "3.2.10"
      ,"org.json4s"                 %% "json4s-ext"         % "3.2.10"
      ,"com.typesafe.scala-logging" %% "scala-logging"      % "3.4.0"
      ,"org.scalatest"              %% "scalatest"          % "3.0.0-RC1"
      ,"com.typesafe"               %  "config"             % "1.3.0"
      ,"org.scala-graph"            %% "graph-core"         % "1.11.2"         // scala library to work with graphs
      ,"org.jgrapht"                %  "jgrapht-core"       % "0.9.0"          // Karma uses java library to work with graphs
      ,"org.json"                   %  "json"               % "20141113"       // dependency for Karma
    )
  )

/**
  * Serene Core module. Contains glue code, servers and communications...
  */
lazy val core = Project(
  id = "serene-core",
  base = file("core")
)
  .settings(commonSettings)
  .settings(
    organization := "au.csiro.data61.serene",
    name := "serene-core",
    version := mainVersion,

    outputStrategy := Some(StdoutOutput),
    parallelExecution in Test := false,
    scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature"),
    resolvers += Resolver.sonatypeRepo("snapshots"),

    // coverageEnabled := true,
    // coverageOutputHTML := true,

    libraryDependencies ++= Seq(
      "org.json4s"                  %% "json4s-jackson"     % "3.2.10"
      ,"org.json4s"                 %% "json4s-native"      % "3.2.10"
      ,"org.json4s"                 %% "json4s-ext"         % "3.2.10"
      ,"org.eclipse.jetty"          %  "jetty-webapp"       % "9.2.10.v20150310" % "container"
      ,"javax.servlet"              %  "javax.servlet-api"  % "3.1.0"            % "provided"
      ,"commons-io"                 %  "commons-io"         % "2.5"
      ,"com.typesafe.scala-logging" %% "scala-logging"      % "3.4.0"
      ,"org.scalatest"              %% "scalatest"          % "3.0.0-RC1"
      ,"com.github.finagle"         %% "finch-core"         % "0.11.1"
      ,"com.github.finagle"         %% "finch-json4s"       % "0.11.1"
      ,"com.github.finagle"         %% "finch-test"         % "0.11.1"
      ,"com.twitter"                %% "finagle-http"       % "6.39.0"
      ,"junit"                      %  "junit"              % "4.12"
      ,"com.typesafe"               %  "config"             % "1.3.0"
      ,"com.github.scopt"           %% "scopt"              % "3.5.0"
    )
  )
  .settings(jetty() : _*)
  .enablePlugins(RpmPlugin, JavaAppPackaging)
  .dependsOn(common)

/**
  * Serene embedding module. Holds the algorithms for generating node embeddings across graphs.
  */
lazy val embedding = Project(
  id = "serene-embedding",
  base = file("embedding")
)
  .settings(commonSettings)
  .settings(
    name := "serene-embedding",
    organization := "au.csiro.data61.serene",
    version := mainVersion,

    libraryDependencies ++= Seq(
      "org.json4s"                  %% "json4s-jackson"     % "3.2.10"
      ,"org.apache.spark"           %% "spark-core"           % "2.1.0"
      ,"org.apache.spark"           %% "spark-sql"            % "2.1.0"
      ,"org.apache.spark"           %% "spark-mllib"          % "2.1.0"
      ,"org.json4s"                 %% "json4s-native"      % "3.2.10"
      ,"org.json4s"                 %% "json4s-ext"         % "3.2.10"
      ,"com.typesafe.scala-logging" %% "scala-logging"      % "3.4.0"
      ,"org.scalatest"              %% "scalatest"          % "3.0.0-RC1"
      ,"com.typesafe"               %  "config"             % "1.3.0"
      ,"org.scala-graph"            %% "graph-core"         % "1.11.2"         // scala library to work with graphs
      ,"org.jgrapht"                %  "jgrapht-core"       % "0.9.0"          // Karma uses java library to work with graphs
      ,"org.json"                   %  "json"               % "20141113"       // dependency for Karma
    )
  )

/**
  * Serene entity module. Controls the entity resolution module.
  */
lazy val entity = Project(
    id = "serene-entity",
    base = file("entity")
  )
  .settings(commonSettings)
  .settings(
    name := "serene-entity",
    organization := "au.csiro.data61.serene",
    version := mainVersion,

    libraryDependencies ++= Seq(
      "org.json4s"                  %% "json4s-jackson"     % "3.2.10"
      ,"org.json4s"                 %% "json4s-native"      % "3.2.10"
      ,"org.json4s"                 %% "json4s-ext"         % "3.2.10"
      ,"com.typesafe.scala-logging" %% "scala-logging"      % "3.4.0"
      ,"org.scalatest"              %% "scalatest"          % "3.0.0-RC1"
      ,"com.typesafe"               %  "config"             % "1.3.0"
      ,"org.scala-graph"            %% "graph-core"         % "1.11.2"         // scala library to work with graphs
      ,"org.jgrapht"                %  "jgrapht-core"       % "0.9.0"          // Karma uses java library to work with graphs
      ,"org.json"                   %  "json"               % "20141113"       // dependency for Karma
    )
  )

/**
  * Serene graph module. Holds graph data structures and algorithms.
  */
lazy val graph = Project(
  id = "serene-graph",
  base = file("graph")
)
  .settings(commonSettings)
  .settings(
    name := "serene-graph",
    organization := "au.csiro.data61.serene",
    version := mainVersion,

    libraryDependencies ++= Seq(
      "org.json4s"                  %% "json4s-jackson"     % "3.2.10"
      ,"org.json4s"                 %% "json4s-native"      % "3.2.10"
      ,"org.json4s"                 %% "json4s-ext"         % "3.2.10"
      ,"com.typesafe.scala-logging" %% "scala-logging"      % "3.4.0"
      ,"org.scalatest"              %% "scalatest"          % "3.0.0-RC1"
      ,"com.typesafe"               %  "config"             % "1.3.0"
      ,"org.scala-graph"            %% "graph-core"         % "1.11.2"         // scala library to work with graphs
      ,"org.jgrapht"                %  "jgrapht-core"       % "0.9.0"          // Karma uses java library to work with graphs
      ,"org.json"                   %  "json"               % "20141113"       // dependency for Karma
    )
  )


/**
  * Serene main module. Pulls in component projects..
  */
lazy val root = Project(
  id = "serene",
  base = file(".")
)
  .settings(commonSettings)
  .settings(
    name := "serene",
    version := mainVersion,
    mainClass in (Compile, run) := Some("au.csiro.data61.serene.core.Serene")
  )
  .aggregate(core, common, algorithm, embedding, entity, graph)
  .dependsOn(core, common, algorithm, embedding, entity, graph)
