import sbt.Compile

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion :=  "2.13.12" //"2.12.15" //"3.3.1"
/*
  [error] 39 |  val slidingTupleUDF = udf((value: Array[String]) => {value.sliding(2).map { case Array(a, b) => (a, b) }.toList})
  [error]    |                                                                                                                   ^
  [error]    |                         No TypeTag available for List[(String, String)]
*/
autoScalaLibrary := false

lazy val root = (project in file("."))
  .settings(
    name := "VirtualInterline"
  )

val sparkVersion = "3.5.0"
libraryDependencies ++= Seq(
  ("org.apache.spark" %% "spark-core" % sparkVersion)   //.cross(CrossVersion.for3Use2_13)
  , ("org.apache.spark" %% "spark-sql" % sparkVersion)  //.cross(CrossVersion.for3Use2_13)
)

// include the 'provided' Spark dependency on the classpath for sbt run
mainClass in Compile := Some("com.travelport.virtualinterline.TheMain")

Compile / run := Defaults.runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner).evaluated
