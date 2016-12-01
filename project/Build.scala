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
  .aggregate(core)
  .dependsOn(core)

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
        ,"au.com.nicta"               %% "data-integration"   % "1.2.0-SNAPSHOT"
      )
    )
    .settings(jetty() : _*)
    .enablePlugins(RpmPlugin, JavaAppPackaging)
}
