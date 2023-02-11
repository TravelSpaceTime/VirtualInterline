package com.travelport.virtualinterline.Analysis



import org.apache.spark.sql.functions.{col, concat, concat_ws, desc, explode_outer, lit, regexp_replace, split, udf}
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

    val groupByColumns = Seq("out_origin_city","out_destination_city","out_origin_airport","out_destination_airport"
      ,"out_num_stops", "out_operating_cxr", "out_via_airports")

    val columns = Seq("out_origin_city", "in_origin_city", "out_segments", "currency", "out_baggage", "out_brand_id", "out_equipments"
      , "out_origin_airport", "out_seats", "out_arrival_date", "out_arrival_time", "out_cabin_class", "out_durations", "out_layovers"
      , "out_num_stops", "out_refund_rule", "out_stop_over", "out_via_airports", "pos_city", "group_id", "in_brand_id", "in_origin_airport"
      , "in_seats", "in_segments", "out_arrival_epoc", "out_destination_city", "out_fare_types", "out_marketing_cxr", "out_operating_cxr"
      , "out_remaining_seats", "split_ticket", "channel_id", "fare", "gds", "id", "in_arrival_date", "in_arrival_time", "in_baggage"
      , "in_durations", "in_equipments", "originalRequest", "out_avail_source", "out_booking_class", "out_flight_numbers", "tax"
      , "validating_cxr", "in_arrival_epoc", "in_cabin_class", "in_destination_city", "in_fare_types", "in_layovers", "in_marketing_cxr"
      , "in_num_stops", "in_operating_cxr", "in_refund_rule", "in_stop_over", "in_via_airports", "out_departure_date", "out_departure_time"
      , "out_fare_components", "pcc", "point_of_sale", "private_fare", "refund_rule", "request_PTC", "response_PTC", "robotic_shop_type"
      , "tax_breakdown", "in_avail_source", "in_booking_class", "in_flight_numbers", "in_remaining_seats", "out_airport_search"
      , "out_arrival_terminal", "out_departure_epoc", "out_incidental_stops", "out_trip_duration_time", "confidence_level"
      , "constricted_search", "in_departure_date", "in_departure_time", "in_fare_components", "out_arrival_time_zone"
      , "out_change_of_gauge", "shop_req_timeStamp", "in_airport_search", "in_arrival_terminal", "in_departure_epoc"
      , "in_incidental_stops", "in_trip_duration_time", "out_destination_airport", "in_arrival_time_zone", "in_change_of_gauge"
      , "out_departure_terminal", "fare_construction_text", "in_departure_terminal", "in_destination_airport"
      , "out_departure_time_zone", "in_departure_time_zone", "tax_break_down_by_PTC", "fare_break_down_by_PTC"
      , "lowest_fare_not_in_result", "out_incidental_stop_airport", "out_incidental_stop_arrival", "private_fare_account_code"
      , "in_incidental_stop_airport", "in_incidental_stop_arrival", "out_codeshare_operating_flight", "out_incidental_stop_departure"
      , "in_codeshare_operating_flight", "out_codeshare_operating_carrier", "in_codeshare_operating_carrier"
      , "in_incidental_stop_departure", "out_availability_connection_indicator", "in_availability_connection_indicator"
    )
    val odv = routesDF.select(groupByColumns.map(c => col(c.toString())): _*
    ).groupBy(groupByColumns.map(c => col(c.toString())): _*).count()

    val ovdDF = odv.orderBy(desc("count"))
    ovdDF
  }

  def viaAirportsAsOnD(spark: SparkSession, ovdDF: DataFrame):DataFrame = {

    val ccODViaA = ovdDF.withColumn("concatOnDVia"
      , concat_ws(","
        ,col("out_origin_city")
        ,col("out_via_airports")
        //,regexp_replace(col("out_via_airports"),"|",",")
        ,col("out_destination_city")
      )
    )

    val regex_delimiter = ","
    val ccToArr = ccODViaA.withColumn("arrOfOnDs", split(col( "concatOnDVia") , regex_delimiter))



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

  def pairwiseAirportsExploded(spark: SparkSession, arrVal: DataFrame):DataFrame ={
    import spark.implicits._
    val explodedDF = arrVal.withColumn("pwOnD",explode_outer($"ncol"))
    //val explodedDF =  arrVal.select(explode_outer($"ncol")).as("explodedPairOnD").distinct
    explodedDF
  }

}
