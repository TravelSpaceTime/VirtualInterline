package com.travelport.virtualinterline


import com.travelport.virtualinterline.Analysis.eStreamingData
//import com.travelport.virtualinterline.Integration.CreateNodesEdgesWeights.generateCombinationsWithMinLength
import com.travelport.virtualinterline.Integration.{CreateNodesEdgesWeights, ReadData, SparkConfiguration}

import org.apache.spark.sql.functions._

import org.apache.spark.sql.types.{ArrayType, StructType, StructField, StringType}
import scala.reflect.runtime.universe._

object TheMain extends App {

  val t1 = System.nanoTime

  val midtData = "/Users/shrikanth.mysore/Downloads/TPData/midtItin/"
  val csvSaveLocation = "/Users/shrikanth.mysore/Downloads/AvailabilityData/SavedResults/"
  val currencyConverterFIleLocation = "/Users/shrikanth.mysore/Downloads/TPData/currencyConvertedTable.csv"
  val EStreamingLogFileLocation = "/Users/shrikanth.mysore/Downloads/TPData/eStreaming15S3/lkndhppnas2900v-2023020900004023-38401.snappy.parquet"

  val spark = SparkConfiguration.sparkSqlConfig()
  import spark.implicits._

  val rdf = new ReadData

  val logEStrParquetDF = rdf.readParquet(spark, EStreamingLogFileLocation)

  val esdf = new eStreamingData

  val shortListAttributes = esdf.collectOutAirportAirline(spark, logEStrParquetDF)
 // shortListAttributes.distinct().show(10,false)

  val df2 = CreateNodesEdgesWeights.createAirportNodes(spark,shortListAttributes)
  df2.distinct.show(10,false)



  val slidingTupleUDF = udf((value: Array[String]) => {value.sliding(2).map { case Array(a, b) => (a, b) }.toList})
  val odTuple2  = df2.withColumn("alp", slidingTupleUDF($"Airport_ondVia"))
  odTuple2.show(10,false)

  val slidingTupleUDF3 = udf((value: Array[String]) => {value.sliding(3).map { case Array(a, b,c) => (a, b,c) }.toList})
  val odTuple3  = odTuple2.withColumn("alp2", slidingTupleUDF3($"Airport_ondVia"))
  odTuple3.filter(col("out_num_stops") === 2).show(10,false)

  /*
  import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("LeastCostPath")
  .getOrCreate()

import org.apache.spark.sql.DataFrame

val vertices: DataFrame = Seq(
  (1, "A"),
  (2, "B"),
  (3, "C"),
  (4, "D"),
  (5, "E")
).toDF("id", "name")

val edges: DataFrame = Seq(
  (1, 2, 1), // (source, target, cost)
  (2, 3, 2),
  (3, 4, 3),
  (4, 5, 4),
  (1, 3, 5)
).toDF("src", "dst", "cost")

import org.graphframes.GraphFrame

val graph = GraphFrame(vertices, edges)

val results = graph.shortestPaths.landmarks(Seq("A")).run()
results.show()

   */
  spark.stop()
  val duration = (System.nanoTime - t1) / 1e9d
  println("\n\ntotal Execution Time : " + duration + " in seconds\n\n")
}

/*
  /opt/homebrew/bin/spark-submit /Users/shrikanth.mysore/Documents/GitProjects/VirtualInterline/target/scala-2.12/virtualinterline_2.12-0.1.0-SNAPSHOT.jar
 /opt/homebrew/bin/spark-submit /Users/shrikanth.mysore/Documents/GitProjects/VirtualInterline/target/scala-3.3.1/virtualinterline_3-0.1.0-SNAPSHOT.jar

 */
