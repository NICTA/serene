import com.earldouglas.xwp.XwpPlugin._
import com.typesafe.sbt.packager.archetypes.JavaAppPackaging
import com.typesafe.sbt.packager.rpm._

//import sbtassembly.Plugin._
import sbt._
import sbt.Keys._
import sbtassembly.AssemblyPlugin.autoImport._


object SereneBuild extends Build {

  lazy val commonSettings = Defaults.coreDefaultSettings ++ Seq(

    assemblyMergeStrategy in assembly := {
      case f if f.startsWith("META-INF") => MergeStrategy.discard
      case f if f.endsWith(".conf") => MergeStrategy.concat
      case f if f.endsWith(".html") => MergeStrategy.first
      case f if f.endsWith(".class") => MergeStrategy.first
      case f =>
        val oldStrategy = (assemblyMergeStrategy in assembly).value
        oldStrategy(f)
    },

    // long file names become an issue on encrypted file systems - this is a weird workaround
    scalacOptions ++= Seq("-Xmax-classfile-name","78"),

    javaOptions in run ++= Seq("-Xms256M", "-Xmx4G", "-XX:MaxPermSize=1024M", "-XX:+UseConcMarkSweepGC"),

    test in assembly := {},

//    fork := true,

    scalaVersion := "2.11.8"
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
      version := "0.1.0",

      mainClass in Compile := Some("au.csiro.data61.core.Serene"),

      mainClass in assembly := Some("au.csiro.data61.core.Serene"),

//      fork := true,

      assemblyJarName in assembly := s"serene-${version.value}.jar"
    )
    .aggregate(core, matcher, ingest)
    .dependsOn(core, matcher, ingest)

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
//      fork := true,
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
        "com.joestelmach"         %  "natty"                 % "0.8",
        "org.apache.spark"        %%  "spark-core"           % "1.6.1",
        "org.apache.spark"        %%  "spark-sql"            % "1.6.1",
        "org.apache.spark"        %%  "spark-mllib"          % "1.6.1"
      ),

      test in assembly := {},

      resolvers ++= Seq("snapshots", "releases").map(Resolver.sonatypeRepo),

      initialCommands in console in Test := "import org.specs2._",

//      javaOptions in run ++= Seq("-Xms256M", "-Xmx2G", "-XX:MaxPermSize=1024M", "-XX:+UseConcMarkSweepGC"),

      javaOptions in test ++= Seq("-Xms256M", "-Xmx2G", "-XX:MaxPermSize=1024M", "-XX:+UseConcMarkSweepGC"),

      fork in Test := true
    )

    lazy val ingest = Project(id="serene-ingest", base=file("ingest"))
      .settings(
        commonSettings,
        version := "0.1",
        libraryDependencies ++= Seq(
          "org.json4s" %% "json4s-native" % "3.3.0"
        )
      )

  /**
    * Serene Core module. Contains glue code, servers and communications...
    */
  lazy val core = Project(
      id = "serene-core",
      base = file("core")
    )
    .settings(sbtassembly.AssemblyPlugin.assemblySettings)
    .settings(commonSettings)
    .settings(
        organization := "au.csiro.data61",
        name := "serene-core",
        version := "0.1",

//        fork := true,
        outputStrategy := Some(StdoutOutput),
        scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature"),
        resolvers += Resolver.sonatypeRepo("snapshots"),
        parallelExecution in Test := false,

        test in assembly := {},

        mainClass in assembly := Some("au.csiro.data61.core.Serene"),

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
          ,"com.github.scopt"           %% "scopt"              % "3.5.0"
        )
      )
    .settings(jetty() : _*)
    .enablePlugins(RpmPlugin, JavaAppPackaging)
    .dependsOn(matcher, ingest)
}
