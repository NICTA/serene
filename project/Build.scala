import com.earldouglas.xwp.XwpPlugin._
import com.typesafe.sbt.packager.archetypes.JavaAppPackaging
import com.typesafe.sbt.packager.rpm._
import sbt._
import sbt.Keys._


object SereneBuild extends Build {

  /**
    * Serene main module. Pulls in component projects..
    */
  lazy val root = Project(
    id = "serene",
    base = file(".")
  )
  .settings(
    name := "serene",
    version := "0.1",
    scalaVersion := "2.11.8",

    mainClass in (Compile, run) := Some("au.csiro.data61.core.Serene")
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
    .settings(

      name := "serene-matcher",
      organization := "au.csiro.data61",
      scalaVersion := "2.11.8",
      fork := true,
      version := "1.2.0-SNAPSHOT",

      libraryDependencies ++= Seq(
        "org.specs2"              %% "specs2-core"           % "2.3.11" % Test,
        "org.specs2"              %% "specs2-matcher-extra"  % "2.3.11" % Test,
        "org.specs2"              %% "specs2-gwt"            % "2.3.11" % Test,
        "org.specs2"              %% "specs2-html"           % "2.3.11" % Test,
        "org.specs2"              %% "specs2-form"           % "2.3.11" % Test,
        "org.specs2"              %% "specs2-scalacheck"     % "2.3.11" % Test,
        "org.specs2"              %% "specs2-mock"           % "2.3.11" % Test exclude("org.mockito", "mockito-core"),
        "org.specs2"              %% "specs2-junit"          % "2.3.11" % Test,
        "com.rubiconproject.oss"  %  "jchronic"              % "0.2.6",
        "org.json4s"              %% "json4s-native"         % "3.3.0",
        "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.8.2",
        "com.typesafe.scala-logging" %% "scala-logging"      % "3.4.0",
        //    "org.json4s"              %% "json4s-jackson"         % "3.2.10",
        "com.joestelmach"         %  "natty"                 % "0.8",
        "org.apache.spark"        %%  "spark-core"           % "1.6.1",
        "org.apache.spark"        %%  "spark-sql"            % "1.6.1",
        "org.apache.spark"        %%  "spark-mllib"          % "1.6.1"
      ),

      resolvers ++= Seq("snapshots", "releases").map(Resolver.sonatypeRepo),

      initialCommands in console in Test := "import org.specs2._",

      javaOptions in run ++= Seq("-Xms256M", "-Xmx2G", "-XX:MaxPermSize=1024M", "-XX:+UseConcMarkSweepGC"),

      javaOptions in test ++= Seq("-Xms256M", "-Xmx2G", "-XX:MaxPermSize=1024M", "-XX:+UseConcMarkSweepGC"),

      fork in Test := true
    )

  /**
    * Serene Core module. Contains glue code, servers and communications...
    */
  lazy val core = Project(
      id = "serene-core",
      base = file("core")
    )
    .settings(

      organization := "au.csiro.data61",
      name := "serene-core",
      version := "0.1",
      scalaVersion := "2.11.8",

      fork := true,
      outputStrategy := Some(StdoutOutput),
      scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature"),
      resolvers += Resolver.sonatypeRepo("snapshots"),
      parallelExecution in Test := false,

      libraryDependencies ++= Seq(
        "org.json4s"                  %% "json4s-jackson"     % "3.3.0"
        ,"org.json4s"                 %% "json4s-native"      % "3.3.0"
        ,"org.json4s"                 %% "json4s-ext"         % "3.3.0"
        ,"ch.qos.logback"             %  "logback-classic"    % "1.1.3"            % "runtime"
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
        //,"au.com.nicta"               %% "data-integration"   % "1.2.0-SNAPSHOT"
      )
    )
    .settings(jetty() : _*)
    .enablePlugins(RpmPlugin, JavaAppPackaging)
    .dependsOn(matcher)
}
