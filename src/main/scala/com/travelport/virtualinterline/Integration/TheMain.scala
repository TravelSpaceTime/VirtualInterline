package com.travelport.virtualinterline.Integration

import com.travelport.virtualinterline.Analysis.OnDconnectionAnalysis
import org.apache.spark.sql.functions.{col, regexp_replace, udf}

object TheMain extends App{

  val t1 = System.nanoTime

  val logData = "/Users/shrikanth.mysore/Downloads/TPData/eStreaming15S3//lkndhppnas2130v-2023020900000456-58118.snappy.parquet"
  val csvSaveLocation = "/Users/shrikanth.mysore/Downloads/AvailabilityData/SavedResults/"
  val spark = SparkConfiguration.sparkSqlConfig()

  import spark.implicits._

  val rdf = ReadData.readParquet(spark,logData)
  rdf.show(1,false)

  val oda = new OnDconnectionAnalysis()
  val ovdDF = oda.outOViaToD(spark,rdf)
  val arrVal = oda.viaAirportsAsOnD(spark,ovdDF)
  val tupleDF = oda.pairwiseAirports(spark, arrVal)
  val expDF = oda.pairwiseAirportsExploded(spark,tupleDF)
  expDF.show(10,false)
  val selOND = expDF.select("pwOnD").distinct()
  selOND.show(100,false)



  spark.stop()
  val duration = (System.nanoTime - t1) / 1e9d
  println("\n\ntotal Execution Time : " + duration + " in seconds\n\n")

  /*
    /opt/homebrew/bin/spark-submit /Users/shrikanth.mysore/Documents/GitProjects/VirtualInterline/target/scala-2.12/virtualinterline_2.12-0.1.0-SNAPSHOT.jar
   */

}

