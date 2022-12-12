package com.travelport.virtualinterline.Integration

import com.travelport.virtualinterline.Analysis.OnDconnectionAnalysis
import org.apache.spark.sql.functions.{col, udf}

object TheMain extends App{

  val t1 = System.nanoTime

  val logData = "/Users/shrikanth.mysore/Downloads/eStreaming15/matched_encoded_v1_5_cut301_20220820/*.parquet"
  val csvSaveLocation = "/Users/shrikanth.mysore/Downloads/AvailabilityData/SavedResults/"
  val spark = SparkConfiguration.sparkSqlConfig()

  import spark.implicits._

  val rdf = ReadData.readParquet(spark,logData)
  rdf.show(1,false)

  val ondV = new OnDconnectionAnalysis()
  val ovdDF = ondV.outOViaToD(spark,rdf)
  val arrVal = ondV.viaAirportsAsOnD(spark,ovdDF)
  val tupleDF = ondV.pairwiseAirports(spark, arrVal)
  tupleDF.show(10,false)



  spark.stop()
  val duration = (System.nanoTime - t1) / 1e9d
  println("\n\ntotal Execution Time : " + duration + " in seconds\n\n")

  /*
    /opt/homebrew/bin/spark-submit /Users/shrikanth.mysore/Documents/GitProjects/VirtualInterline/target/scala-2.12/virtualinterline_2.12-0.1.0-SNAPSHOT.jar
   */

}

