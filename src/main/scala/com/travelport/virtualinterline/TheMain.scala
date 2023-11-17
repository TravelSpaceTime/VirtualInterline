package com.travelport.virtualinterline


import com.travelport.virtualinterline.Analysis.eStreamingData
import org.apache.spark.sql.functions
//import com.travelport.virtualinterline.Integration.CreateNodesEdgesWeights.generateCombinationsWithMinLength
import com.travelport.virtualinterline.Integration.{CreateNodesEdgesWeights, ReadData, SparkConfiguration}
import org.apache.spark.sql.functions._

object TheMain extends App {

  val t1 = System.nanoTime

  val midtData = "/Users/shrikanth.mysore/Downloads/TPData/midtItin/"
  val csvSaveLocation = "/Users/shrikanth.mysore/Downloads/AvailabilityData/SavedResults/"
  val currencyConverterFIleLocation = "/Users/shrikanth.mysore/Downloads/TPData/currencyConvertedTable.csv"
  val EStreamingLogFileLocation = "/Users/shrikanth.mysore/Downloads/TPData/eStreaming15S3/*.parquet" // lkndhppnas2900v-2023020900004023-38401.snappy.parquet"
  //val airlinePricingAggregates = "/Users/shrikanth.mysore/Downloads/multiCityAirlinePricingAggregates/*.parquet"

  val spark = SparkConfiguration.sparkSqlConfig()

  import spark.implicits._

  val rdf = new ReadData

  val logEStrParquetDF = rdf.readParquet(spark, EStreamingLogFileLocation)

  val esdf = new eStreamingData

  val shortListAttributes = esdf.collectOutAirportAirline(spark, logEStrParquetDF)
  val df2 = CreateNodesEdgesWeights.createAirportNodes(spark, shortListAttributes)

  val slidingTupleUDF = udf((value: Array[String]) => {
    value.sliding(2).map { case Array(a, b) => (a, b) }.toList
  })
  val slidingTupleUDF3 = udf((value: Array[String]) => {
    value.sliding(3).map { case Array(a, b, c) => (a, b, c) }.toList
  })

  val odTuple4 = df2
    .withColumn("alp", slidingTupleUDF($"Airport_ondVia"))
    .withColumn("alp2", slidingTupleUDF3($"Airport_ondVia"))

  val explodedDf = odTuple4
    .withColumn("exploded_data_alp", functions.explode(col("alp")))
    .withColumn("exploded_data_alp2", functions.explode(col("alp2")))
    .withColumn("combinedListcxrAirport", functions.arrays_zip(col("out_marketing_cxr"), col("alp")))


  //explodedDf.filter(col("out_num_stops") === 3).show(10,false)

  val pathsDF = explodedDf.select("out_origin_city", "out_via_airports", "out_destination_city"
    , "validating_cxr", "out_marketing_cxr", "out_num_stops"
    , "exploded_data_alp", "exploded_data_alp2"
    , "combinedListcxrAirport", "currency", "fare")

  val pathsWithCXR = pathsDF.withColumn("exploded_CXR_Airports_toJoin", functions.explode(col("combinedListcxrAirport")))
    .withColumn("combinedOND", functions.struct("out_origin_city", "out_destination_city"))

  val cxrOndtoMatch = pathsWithCXR.withColumn("combinedCxrOnD", functions.struct("validating_cxr", "combinedOND"))

  val tojoinDF = cxrOndtoMatch.filter(col("out_num_stops") === 3).select("out_origin_city", "out_via_airports", "out_destination_city"
    , "validating_cxr", "out_marketing_cxr", "out_num_stops"
    , "exploded_data_alp", "exploded_data_alp2"
    , "combinedListcxrAirport", "currency", "fare","combinedCxrOnD","combinedOND")

  cxrOndtoMatch.filter(col("out_num_stops") === 3).show(10,false)
  tojoinDF.show(10, false)

  val tableA = tojoinDF.select("combinedOND").distinct()
  tableA.show(10,false)
  val tableB = tojoinDF.select("combinedOND","validating_cxr", "fare", "currency")
  tableB.show(10,false)
  val collectedOccurrences = tableA
    .join(tableB,tableB("combinedOND") === tableA("combinedOND"))

  collectedOccurrences.show(10, false)

  spark.stop()
  val duration = (System.nanoTime - t1) / 1e9d
  println("\n\ntotal Execution Time : " + duration + " in seconds\n\n")
}

/*
  /opt/homebrew/bin/spark-submit /Users/shrikanth.mysore/Documents/GitProjects/VirtualInterline/target/scala-2.12/virtualinterline_2.12-0.1.0-SNAPSHOT.jar
 /opt/homebrew/bin/spark-submit /Users/shrikanth.mysore/Documents/GitProjects/VirtualInterline/target/scala-3.3.1/virtualinterline_3-0.1.0-SNAPSHOT.jar

 */
