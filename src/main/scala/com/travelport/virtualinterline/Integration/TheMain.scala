package com.travelport.virtualinterline.Integration

import com.travelport.virtualinterline.Analysis.OnDconnectionAnalysis
import org.apache.spark.sql.functions.broadcast

object TheMain extends App {

  val t1 = System.nanoTime

  val logData = "/Users/shrikanth.mysore/Downloads/TPData/eStreaming15S3//lkndhppnas2130v-2023020900000456-58118.snappy.parquet"
  /*

+---------------+---------------+---------------+---+---+---+--------+--------+----------+----------+---+------------+------------+-------------+------------------+------------+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---------------------+----------------------+-------------------+--------------------+--------+---------+-------+--------+----------+-----------+------------------+------+--------------+
|transactionDate|transactionHour|transactionTime|gds|pcc|pos|agentSrc|sourceID|vendorCode|vendorType|cxr|flightNumber|deptDateTime|originAirport|destinationAirport|inventorySrc|A  |B  |C  |D  |E  |F  |G  |H  |I  |J  |K  |L  |M  |N  |O  |P  |Q  |R  |S  |T  |U  |V  |W  |X  |Y  |Z  |rawAvailability      |transactionDateTimeStr|transactionDateTime|deptDateTimeFormated|deptYear|deptMonth|deptDay|deptHour|deptMinute|deptWeekday|hoursTillDeparture|market|airline_market|
+---------------+---------------+---------------+---+---+---+--------+--------+----------+----------+---+------------+------------+-------------+------------------+------------+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---------------------+----------------------+-------------------+--------------------+--------+---------+-------+--------+----------+-----------+------------------+------+--------------+
|20220828       |21             |293626         |P  |5H6|CO |        |        |          |          |LA |4103        |202210041450|CTG          |BOG               |P           |0  |7  |0  |0  |0  |0  |7  |7  |0  |0  |7  |7  |7  |7  |7  |7  |7  |0  |7  |0  |0  |7  |7  |7  |7  |0  |0777777777777777     |20220828-212936       |2022-08-28 21:29:36|2022-10-04 14:50:00 |2022    |10       |4      |14      |50        |Tue        |881               |CTGBOG|LA_CTGBOG     |
|20220828       |21             |294207         |P  |RUL|HK |        |        |          |          |PR |468         |202209241405|MNL          |ICN               |P           |0  |9  |9  |7  |9  |0  |0  |9  |3  |9  |9  |9  |9  |9  |9  |0  |9  |0  |9  |9  |9  |9  |9  |9  |9  |2  |997993999999999999992|20220828-212942       |2022-08-28 21:29:42|2022-09-24 14:05:00 |2022    |9        |24     |14      |5         |Sat        |641               |MNLICN|PR_MNLICN     |
+---------------+---------------+---------------+---+---+---+--------+--------+----------+----------+---+------------+------------+-------------+------------------+------------+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---------------------+----------------------+-------------------+--------------------+--------+---------+-------+--------+----------+-----------+------------------+------+--------------+
only showing top 2 rows
+----------------+--------+----------------------------+------------------+---+----+-------------+------------------+------------+-------------------------+-----------+-----------------+----------------+--------+------------+--------+-------+------+--------------+----------------------+---------------------+-----------------------------------------------------------------+---------------------------+------------+----------+-------------------------+------------------------------------------------------------------------------+-------------+----------------------+------------+------------------+-----------------------+---------------+--------------------+------------------+------------------+----------------+----------------+-----------------------+---------------------+------------------+----------------+----------------------+--------------------+------------------+-----------------+-----------------+------------------+-----------------+---------------+---------+----------------+--------------+------------+-------------------+--------------+-----------+---------------+--------------------+---------------------------+-----------------------------+---------------------------+-------------------+-------------+-------------------------------------+-------------------+------------+-------------------------------+------------------------------+----------------+-------------+------------+---------------------+-----------+-----------------+----------------------+--------------+-------------------+-----------------+-----------------+---------------+---------------+----------------------+--------------------+-----------------+---------------+---------------------+-------------------+-----------------+----------------+----------------+-----------------+----------------+--------------+--------+---------------+-------------+-----------+--------------------------------------------+-------------+----------+--------------+-------------------+--------------------------+----------------------------+--------------------------+------------------+------------+------------------------------------+------------------+-----------+------------------------------+-----------------------------+---------------+------------+
|id              |group_id|originalRequest             |shop_req_timeStamp|gds|pcc |point_of_sale|constricted_search|split_ticket|lowest_fare_not_in_result|request_PTC|robotic_shop_type|confidence_level|pos_city|private_fare|currency|fare   |tax   |validating_cxr|fare_break_down_by_PTC|tax_break_down_by_PTC|tax_breakdown                                                    |refund_rule                |response_PTC|channel_id|private_fare_account_code|fare_construction_text                                                        |out_durations|out_trip_duration_time|out_layovers|out_origin_airport|out_destination_airport|out_origin_city|out_destination_city|out_departure_date|out_departure_time|out_arrival_date|out_arrival_time|out_departure_time_zone|out_arrival_time_zone|out_departure_epoc|out_arrival_epoc|out_departure_terminal|out_arrival_terminal|out_airport_search|out_marketing_cxr|out_operating_cxr|out_flight_numbers|out_booking_class|out_cabin_class|out_seats|out_avail_source|out_equipments|out_segments|out_fare_components|out_fare_types|out_baggage|out_refund_rule|out_incidental_stops|out_incidental_stop_airport|out_incidental_stop_departure|out_incidental_stop_arrival|out_change_of_gauge|out_stop_over|out_availability_connection_indicator|out_remaining_seats|out_brand_id|out_codeshare_operating_carrier|out_codeshare_operating_flight|out_via_airports|out_num_stops|in_durations|in_trip_duration_time|in_layovers|in_origin_airport|in_destination_airport|in_origin_city|in_destination_city|in_departure_date|in_departure_time|in_arrival_date|in_arrival_time|in_departure_time_zone|in_arrival_time_zone|in_departure_epoc|in_arrival_epoc|in_departure_terminal|in_arrival_terminal|in_airport_search|in_marketing_cxr|in_operating_cxr|in_flight_numbers|in_booking_class|in_cabin_class|in_seats|in_avail_source|in_equipments|in_segments|in_fare_components                          |in_fare_types|in_baggage|in_refund_rule|in_incidental_stops|in_incidental_stop_airport|in_incidental_stop_departure|in_incidental_stop_arrival|in_change_of_gauge|in_stop_over|in_availability_connection_indicator|in_remaining_seats|in_brand_id|in_codeshare_operating_carrier|in_codeshare_operating_flight|in_via_airports|in_num_stops|
+----------------+--------+----------------------------+------------------+---+----+-------------+------------------+------------+-------------------------+-----------+-----------------+----------------+--------+------------+--------+-------+------+--------------+----------------------+---------------------+-----------------------------------------------------------------+---------------------------+------------+----------+-------------------------+------------------------------------------------------------------------------+-------------+----------------------+------------+------------------+-----------------------+---------------+--------------------+------------------+------------------+----------------+----------------+-----------------------+---------------------+------------------+----------------+----------------------+--------------------+------------------+-----------------+-----------------+------------------+-----------------+---------------+---------+----------------+--------------+------------+-------------------+--------------+-----------+---------------+--------------------+---------------------------+-----------------------------+---------------------------+-------------------+-------------+-------------------------------------+-------------------+------------+-------------------------------+------------------------------+----------------+-------------+------------+---------------------+-----------+-----------------+----------------------+--------------+-------------------+-----------------+-----------------+---------------+---------------+----------------------+--------------------+-----------------+---------------+---------------------+-------------------+-----------------+----------------+----------------+-----------------+----------------+--------------+--------+---------------+-------------+-----------+--------------------------------------------+-------------+----------+--------------+-------------------+--------------------------+----------------------------+--------------------------+------------------+------------+------------------------------------+------------------+-----------+------------------------------+-----------------------------+---------------+------------+
|1757309668536243|228     |20230331TPESHA20230407SHATPE|1675901093        |1G |804U|TW           |true              |false       |false                    |[ADT1]     |0                |0               |TPE     |false       |TWD     |22915.0|2515.0|NX            |[22915.0]             |[2515.0]             |[{TW, 500.0}, {CN, 399.0}, {MO, 112.0}, {YQ, 752.0}, {YR, 752.0}]|Can-1500TWD-AT/Itinchg-0-AT|[ADT]       |0         |[]                       |TPE BR SHA 375.25Y1TW NX X/MFM NX TPE 290.41QEE6MTW2 NUC665.66END ROE30.646005|[115]        |115                   |[]          |TPE               |PVG                    |TPE            |SHA                 |20230331          |1245              |20230331        |1440            |CT                     |CT                   |1680237900        |1680244800      |2                     |2                   |false             |[BR]             |[BR]             |[752]             |[Y]              |[E]            |[1]      |[P]             |[77W]         |[]          |[[[{Y1TW, null}]]] |[]            |[2PC]      |null           |[]                  |[]                         |[]                           |[]                         |[]                 |[]           |[]                                   |[009]              |[1169262]   |[]                             |[]                            |[]              |0            |[170, 115]  |2350                 |[]         |SHA              |TPE                   |SHA           |TPE                |20230407         |1355             |20230408       |930            |CT                    |CT                  |1680846900       |1680917400     |1                    |1                  |false            |[NX, NX]        |[NX, NX]        |[109, 632]       |[Q, Q]          |[E, E]        |[1]     |[A, A]         |[321, 321]   |[]         |[[[{QEE6MTW2, null}]], [[{QEE6MTW2, null}]]]|[]           |[20K, ]   |null          |[0]                |[0]                       |[0]                         |[0]                       |[, ]              |[, , ]      |[, ]                                |[009, 009]        |[323665, ] |[, ]                          |[, ]                         |[MFM]          |1           |
+----------------+--------+----------------------------+------------------+---+----+-------------+------------------+------------+-------------------------+-----------+-----------------+----------------+--------+------------+--------+-------+------+--------------+----------------------+---------------------+-----------------------------------------------------------------+---------------------------+------------+----------+-------------------------+------------------------------------------------------------------------------+-------------+----------------------+------------+------------------+-----------------------+---------------+--------------------+------------------+------------------+----------------+----------------+-----------------------+---------------------+------------------+----------------+----------------------+--------------------+------------------+-----------------+-----------------+------------------+-----------------+---------------+---------+----------------+--------------+------------+-------------------+--------------+-----------+---------------+--------------------+---------------------------+-----------------------------+---------------------------+-------------------+-------------+-------------------------------------+-------------------+------------+-------------------------------+------------------------------+----------------+-------------+------------+---------------------+-----------+-----------------+----------------------+--------------+-------------------+-----------------+-----------------+---------------+---------------+----------------------+--------------------+-----------------+---------------+---------------------+-------------------+-----------------+----------------+----------------+-----------------+----------------+--------------+--------+---------------+-------------+-----------+--------------------------------------------+-------------+----------+--------------+-------------------+--------------------------+----------------------------+--------------------------+------------------+------------+------------------------------------+------------------+-----------+------------------------------+-----------------------------+---------------+------------+
only showing top 1 row



val columns = Seq([out_origin_city, in_origin_city, out_segments, currency, out_baggage, out_brand_id, out_equipments
, out_origin_airport, out_seats, out_arrival_date, out_arrival_time, out_cabin_class, out_durations, out_layovers
, out_num_stops, out_refund_rule, out_stop_over, out_via_airports, pos_city, group_id, in_brand_id, in_origin_airport
, in_seats, in_segments, out_arrival_epoc, out_destination_city, out_fare_types, out_marketing_cxr, out_operating_cxr
, out_remaining_seats, split_ticket, channel_id, fare, gds, id, in_arrival_date, in_arrival_time, in_baggage
, in_durations, in_equipments, originalRequest, out_avail_source, out_booking_class, out_flight_numbers, tax
, validating_cxr, in_arrival_epoc, in_cabin_class, in_destination_city, in_fare_types, in_layovers, in_marketing_cxr
, in_num_stops, in_operating_cxr, in_refund_rule, in_stop_over, in_via_airports, out_departure_date, out_departure_time
, out_fare_components, pcc, point_of_sale, private_fare, refund_rule, request_PTC, response_PTC, robotic_shop_type
, tax_breakdown, in_avail_source, in_booking_class, in_flight_numbers, in_remaining_seats, out_airport_search
, out_arrival_terminal, out_departure_epoc, out_incidental_stops, out_trip_duration_time, confidence_level
, constricted_search, in_departure_date, in_departure_time, in_fare_components, out_arrival_time_zone,
 out_change_of_gauge, shop_req_timeStamp, in_airport_search, in_arrival_terminal, in_departure_epoc
 , in_incidental_stops, in_trip_duration_time, out_destination_airport, in_arrival_time_zone, in_change_of_gauge
 , out_departure_terminal, fare_construction_text, in_departure_terminal, in_destination_airport
 , out_departure_time_zone, in_departure_time_zone, tax_break_down_by_PTC, fare_break_down_by_PTC
 , lowest_fare_not_in_result, out_incidental_stop_airport, out_incidental_stop_arrival, private_fare_account_code
 , in_incidental_stop_airport, in_incidental_stop_arrival, out_codeshare_operating_flight, out_incidental_stop_departure
 , in_codeshare_operating_flight, out_codeshare_operating_carrier, in_codeshare_operating_carrier
 , in_incidental_stop_departure, out_availability_connection_indicator, in_availability_connection_indicator];
)
   */

  val csvSaveLocation = "/Users/shrikanth.mysore/Downloads/AvailabilityData/SavedResults/"
  val spark = SparkConfiguration.sparkSqlConfig()

  import spark.implicits._

  val rdf = ReadData.readParquet(spark, logData)
  rdf.show(1, false)

  val oda = new OnDconnectionAnalysis()
  val ovdDF = oda.outOViaToD(spark, rdf)
  val arrVal = oda.viaAirportsAsOnD(spark, ovdDF)
  val tupleDF = oda.pairwiseAirports(spark, arrVal)
  val expDF = oda.pairwiseAirportsExploded(spark, tupleDF).sort("out_origin_airport")
  expDF.show(10, false)
  val selOND = expDF.select("pwOnD").distinct().sort("pwOnD").toDF()
  selOND.show(10, false)

  val pwOND = selOND.withColumn("pwOrigin", $"pwOnD._1")
    .withColumn("pwDest", $"pwOnD._2")

  pwOND.show(10, false)

  val skewJoined = expDF.join(
    broadcast(pwOND),
    (pwOND.col("pwOrigin") ===expDF.col("out_origin_airport"))
      && (pwOND.col("pwDest") === expDF.col("out_destination_airport"))
  )

  skewJoined.show(10, false)


  spark.stop()
  val duration = (System.nanoTime - t1) / 1e9d
  println("\n\ntotal Execution Time : " + duration + " in seconds\n\n")

  /*
    /opt/homebrew/bin/spark-submit /Users/shrikanth.mysore/Documents/GitProjects/VirtualInterline/target/scala-2.12/virtualinterline_2.12-0.1.0-SNAPSHOT.jar
   */

}

