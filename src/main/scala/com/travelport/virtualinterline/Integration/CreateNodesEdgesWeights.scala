package com.travelport.virtualinterline.Integration

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
object CreateNodesEdgesWeights {
  def createNodes(spark: SparkSession, AirportCombo:DataFrame):DataFrame = {
    import spark.implicits._
    val df2 = AirportCombo.withColumn("Airport_List", 
      concat_ws(", ", $"out_origin_city", $"out_via_airports",$"out_destination_city"))
      .drop("out_origin_city", "out_via_airports","out_destination_city")
    df2
  }
  def createEdges(OrigDestCost:DataFrame):DataFrame = {
    OrigDestCost
  }

}
