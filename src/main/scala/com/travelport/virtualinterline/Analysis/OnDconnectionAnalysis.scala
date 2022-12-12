package com.travelport.virtualinterline.Analysis



import org.apache.spark.sql.functions.{col, concat, concat_ws, desc, lit, regexp_replace, split, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}

class OnDconnectionAnalysis {
  /*
  In a Virtual Interline, usually unnatural itineraries are put together.
  Initially this is being done just for Air, but then will be extended to other modes of transport.

  1) Separate the out and the in itineraries
  2) Separate the stops on each itinerary
  3) Compare each leg of the itinerary
  4) Generate all possible combinations that connect the airports with airlines
  5) the sum total of the legs to put an itinerary that can make up of many connections
  6) A page rank algorithm can be a starting point for picking connections with probability of conversion
  7) some long tail connections may have cheaper options - should check

   */
  def outOViaToD(spark: SparkSession, routesDF: DataFrame):DataFrame = {
    import spark.implicits._

    val groupByColumns = Seq("outOriginCity","outDestinationCity","outOriginAirport","outDestinationAirport"
      ,"outNumStops", "outOperatingCxr", "outViaAirports")

    val odv = routesDF.select(groupByColumns.map(c => col(c.toString())): _*
    ).groupBy(groupByColumns.map(c => col(c.toString())): _*).count()

    val ovdDF = odv.orderBy(desc("count"))
    ovdDF
  }

  def viaAirportsAsOnD(spark: SparkSession, ovdDF: DataFrame):DataFrame = {

    val ccODViaA = ovdDF.withColumn("concatOnDVia"
      , concat_ws(","
        ,col("outOriginCity")
        ,col("outViaAirports")
        ,col("outDestinationCity")
      )
    )
    val ccToArr = ccODViaA.withColumn("arrOfOnDs",split(col( "concatOnDVia") , ","))

    ccToArr.show(10, false)
    ccToArr
  }

  def pairwiseAirports(spark: SparkSession, arrVal: DataFrame):DataFrame = {
    import spark.implicits._
    val slide = udf((value: Seq[String]) => {
      value.toList.sliding(2).map { case List(a, b) => (a, b) }.toList
    })
    /*
    https://stackoverflow.com/questions/57550310/how-to-convert-spark-dataframe-array-to-tuple
     */

    val tupleDF = arrVal.withColumn("ncol",slide($"arrOfOnDs"))

    tupleDF
  }
  def viaAirportsToD():Unit = {}
  def viaToViaAirport():Unit = {}
  def oToAnyAirport():Unit = {}
  def AnyAirportToD():Unit = {}

}
