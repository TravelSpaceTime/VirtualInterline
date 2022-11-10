ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.15"

lazy val root = (project in file("."))
  .settings(
    name := "VirtualInterline"
  )

val sparkVersion = "3.3.1"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion
  , "org.apache.spark" %% "spark-sql" % sparkVersion
,  "org.apache.spark" %% "spark-graphx" % sparkVersion
  , "org.apache.sedona" %% "sedona-core-3.0" % "1.2.1-incubating" % "provided"

)
