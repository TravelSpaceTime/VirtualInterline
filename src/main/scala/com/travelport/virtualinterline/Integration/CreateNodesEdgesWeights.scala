package com.travelport.virtualinterline.Integration

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

object CreateNodesEdgesWeights {
  def createAirportNodes(spark: SparkSession, AirportCombo: DataFrame): DataFrame = {
    import spark.implicits._
    val df2 = AirportCombo.withColumn("Airport_ondVia",
      split(concat_ws(",", $"out_origin_city", $"out_via_airports", $"out_destination_city"),","))

    df2
  }

  /*
  may need to create separate in and out nodes and direct them if there is a significant difference in the graph for out vs in.
  */



  // Function to create a UDF for sliding tuples
  def createSlidingTupleUDF(n: Int) = udf { (value: Array[String]) =>
    value.sliding(n).collect { case arr if arr.length == n => arr }.toList
  }

  // Function to apply the UDF and add a new column to the DataFrame
  def applySlidingTupleUDF(df: DataFrame, colName: String, udf: Column) =
    df.withColumn(colName, udf(col(colName)))

  def createEdges(spark: SparkSession, df2: DataFrame): DataFrame = {

    val df3 = df2.withColumn(
      "Airport_ondVia2",
      expr("""
        filter(
            transform(
                flatten(
                    transform(
                        Airport_ondVia,
                        x -> arrays_zip(
                            array_repeat(x, size(Airport_ondVia)),
                            Airport_ondVia
                        )
                    )
                ),
                x -> array(x['0'], x['Airport_ondVia'])
            ),
            x -> x[0] < x[1]
        )
    """)
    )



    df2
  }

  def createpartialPaths(OriginDestination: DataFrame): DataFrame = {
    OriginDestination
  }

}
