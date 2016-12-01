import sbt.Process._

name := "data-integration"

organization := "au.com.nicta"

scalaVersion := "2.11.8"

fork := true

version := "1.2.0-SNAPSHOT"


/************************************************************
 *                        DEPENDENCIES                      *
 ************************************************************/
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
)


//dependencyOverrides ++= Set(
//    "com.fasterxml.jackson.core" % "jackson-databind" % "2.4.4"
//)

resolvers ++= Seq("snapshots", "releases").map(Resolver.sonatypeRepo)

initialCommands in console in Test := "import org.specs2._"

javaOptions in run ++= Seq("-Xms256M", "-Xmx2G", "-XX:MaxPermSize=1024M", "-XX:+UseConcMarkSweepGC")

javaOptions in test ++= Seq("-Xms256M", "-Xmx2G", "-XX:MaxPermSize=1024M", "-XX:+UseConcMarkSweepGC")

fork in Test := true

/************************************************************
 *                         ASSEMBLY                         *
 ************************************************************/

val copyDeps = TaskKey[Unit]("copy-dependencies", "Copies all jar dependencies into a lib folder.")

copyDeps <<= (update, crossTarget, scalaVersion) map {
    (updateReport, out, scalaVer) =>
    updateReport.allFiles foreach { srcPath =>
        val destPath = out / "lib" / srcPath.getName
        IO.copyFile(srcPath, destPath, preserveLastModified=true)
    }
}

val assembly = TaskKey[Unit]("assembly", "Create archive containing the binaries, dependencies, and shell scripts.")

assembly <<= (copyDeps, packageBin in Compile) map {(_, jarPath) =>
    println("Creating archive ...")

    //delete old archive
    if(Process("test -e prototype").! == 0) {
        Process("rm -rf prototype").!
    }    

    Process("mkdir prototype").!
    Process(s"cp $jarPath ./prototype/prototype.jar").!

    //copy the dependencies
    Process(Seq("bash", "-c", "cp -R target/scala-2.11/lib ./prototype/lib")).!
    Process(Seq("bash", "-c", "cp -R lib ./prototype")).!

    //Additional Netty dependency causing problems!
    Process(Seq("bash", "-c", "rm ./prototype/lib/netty-3.2.2.Final.jar")).!

    //copy the modified scripts to suit the specific platform
    println("Copying scripts for multiclass schema matcher...")
    Process(Seq("bash", "-c", "cp -R ./dirstruct/semantic_type_classifier ./prototype/")).!

    //create some dirs
    Process(Seq("bash", "-c", "mkdir ./prototype/semantic_type_classifier/repo/models")).!
    Process(Seq("bash", "-c", "mkdir ./prototype/semantic_type_classifier/repo/labels/predicted")).!

    //set the filename to indicate the platform it was meant for
    val filename = "prototype-universal.tar.gz"

    //final packaging
    Process(
        Seq("bash", "-c", s"tar --exclude='._*' --exclude='.DS_Store' -czf $filename prototype"),
        None,
        "COPY_EXTENDED_ATTRIBUTES_DISABLE" -> "true",
        "COPYFILE_DISABLE" -> "true"
    ).!

    println(s"Archive created: $filename")
}


