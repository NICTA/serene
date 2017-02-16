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

val mainVersion = "0.1.0"

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
    "org.apache.spark"            %%  "spark-mllib"          % "2.1.0"
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
    mainClass := Some("au.csiro.data61.core.Serene")
  )
  .aggregate(core, matcher)
  .dependsOn(core, matcher)

/**
  * Schema Matcher module
  */
lazy val matcher = Project(
    id = "serene-matcher",
    base = file("matcher")
  )
  .settings(commonSettings)
  .settings(
    name := "serene-matcher",
    organization := "au.csiro.data61",
    version := "1.2.0-SNAPSHOT",

    libraryDependencies ++= Seq(
      "org.specs2"                  %% "specs2-core"           % "3.7" % Test,
      "org.specs2"                  %% "specs2-matcher-extra"  % "3.7" % Test,
      "org.specs2"                  %% "specs2-html"           % "3.7" % Test,
      "org.specs2"                  %% "specs2-form"           % "3.7" % Test,
      "org.specs2"                  %% "specs2-scalacheck"     % "3.7" % Test,
      "org.specs2"                  %% "specs2-mock"           % "3.7" % Test exclude("org.mockito", "mockito-core"),
      "org.specs2"                  %% "specs2-junit"          % "3.7" % Test,
      "com.rubiconproject.oss"      %  "jchronic"              % "0.2.6",
      "org.json4s"                  %% "json4s-native"         % "3.2.10",
      "com.typesafe.scala-logging"  %% "scala-logging"         % "3.4.0",
      "com.joestelmach"             %  "natty"                 % "0.8"
    ),

    resolvers ++= Seq("snapshots", "releases").map(Resolver.sonatypeRepo),

    initialCommands in console in Test := "import org.specs2._",

    fork in Test := true
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
      organization := "au.csiro.data61",
      name := "serene-core",
      version := mainVersion,

      outputStrategy := Some(StdoutOutput),
      scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature"),
      resolvers += Resolver.sonatypeRepo("snapshots"),
      parallelExecution in Test := false,

      mainClass := Some("au.csiro.data61.core.Serene"),

      libraryDependencies ++= Seq(
        "org.json4s"                  %% "json4s-jackson"     % "3.2.10"
        ,"org.json4s"                 %% "json4s-native"      % "3.2.10"
        ,"org.json4s"                 %% "json4s-ext"         % "3.2.10"
        ,"org.eclipse.jetty"          %  "jetty-webapp"       % "9.2.10.v20150310" % "container"
        ,"javax.servlet"              %  "javax.servlet-api"  % "3.1.0"            % "provided"
        ,"commons-io"                 %  "commons-io"         % "2.5"
        ,"com.typesafe.scala-logging" %% "scala-logging"      % "3.4.0"
        ,"org.scalatest"              %% "scalatest"          % "3.0.0-RC1"
        ,"com.github.tototoshi"       %% "scala-csv"          % "1.3.1"
        ,"com.github.finagle"         %% "finch-core"         % "0.10.0"
        ,"com.github.finagle"         %% "finch-json4s"       % "0.10.0"
        ,"com.github.finagle"         %% "finch-test"         % "0.10.0"
        ,"com.twitter"                %% "finagle-http"       % "6.35.0"
        ,"junit"                      %  "junit"              % "4.12"
        ,"com.typesafe"               %  "config"             % "1.3.0"
        ,"com.github.scopt"           %% "scopt"              % "3.5.0"
      )
    )
  .settings(jetty() : _*)
  .enablePlugins(RpmPlugin, JavaAppPackaging)
  .dependsOn(matcher)

