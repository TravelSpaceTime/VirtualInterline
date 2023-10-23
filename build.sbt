ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion :=  "3.3.1" //"2.12.15"
autoScalaLibrary := false
//crossScalaVersions := Seq("2.12.5", "3.3.1")
lazy val root = (project in file("."))
  .settings(
    name := "VirtualInterline"
  )

val sparkVersion = "3.5.0"
libraryDependencies ++= Seq(
  ("org.apache.spark" %% "spark-core" % sparkVersion) .cross(CrossVersion.for3Use2_13)
  , ("org.apache.spark" %% "spark-sql" % sparkVersion) .cross(CrossVersion.for3Use2_13)
)

// include the 'provided' Spark dependency on the classpath for
//sbt run

Compile / run := Defaults.runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner).evaluated
