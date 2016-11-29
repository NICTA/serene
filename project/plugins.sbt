scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

addSbtPlugin("com.earldouglas"  % "xsbt-web-plugin" % "1.1.0")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.8.0")

addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.0.0-RC1")

// IDE settings
//addSbtPlugin("com.github.mpeltonen"    % "sbt-idea"          % "1.6.0")