scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

addSbtPlugin("com.earldouglas"  % "xsbt-web-plugin" % "1.1.0")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.3")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.8.0")

addSbtPlugin("com.typesafe.sbt" % "sbt-native-packager" % "1.0.0-RC1")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.8.2")

// IDE settings
addSbtPlugin("com.github.mpeltonen"    % "sbt-idea"          % "1.6.0")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.0")