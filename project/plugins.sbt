scalacOptions ++= Seq("-unchecked", "-deprecation", "-feature")

addSbtPlugin("com.earldouglas"  % "xsbt-web-plugin" % "1.1.0")

addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "0.8.0")

// IDE settings
//addSbtPlugin("com.github.mpeltonen"    % "sbt-idea"          % "1.6.0")