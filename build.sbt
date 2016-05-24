
lazy val scalatraVersion = "2.3.1"

lazy val root = (project in file(".")).settings(

  organization := "au.csiro.data61",

  name := "matcher",

  version := "0.1.0-SNAPSHOT",

  scalaVersion := "2.11.7",

  scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature"),

  resolvers += Resolver.sonatypeRepo("snapshots"),

  libraryDependencies ++= Seq(
    "org.scalatra"      %% "scalatra"          % scalatraVersion,
    "org.scalatra"      %% "scalatra-scalate"  % scalatraVersion,
    "org.scalatra"      %% "scalatra-specs2"   % scalatraVersion    % "test",
    "org.scalatra"      %% "scalatra-json"     % scalatraVersion,
    "org.json4s"        %% "json4s-jackson"    % "3.3.0",
    "org.json4s"        %% "json4s-native"     % "3.3.0",
    "ch.qos.logback"    %  "logback-classic"   % "1.1.3"            % "runtime",
    "org.eclipse.jetty" %  "jetty-webapp"      % "9.2.10.v20150310" % "container",
    "javax.servlet"     %  "javax.servlet-api" % "3.1.0"            % "provided",
    "commons-io"        %  "commons-io"        % "2.5",
    "com.typesafe.play" %% "play-json"         % "2.5.3"


    ,"com.github.finagle" %% "finch-core" % "0.10.0"
    ,"com.github.finagle" %% "finch-json4s" % "0.10.0"

  )
).settings(jetty(): _*)