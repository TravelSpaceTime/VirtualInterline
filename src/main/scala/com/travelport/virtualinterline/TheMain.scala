package com.travelport.virtualinterline


import com.travelport.virtualinterline.Analysis.eStreamingData
import com.travelport.virtualinterline.Integration.{ReadData, SparkConfiguration}



object TheMain extends App {

  val t1 = System.nanoTime

  val midtData = "/Users/shrikanth.mysore/Downloads/TPData/midtItin/"
  val csvSaveLocation = "/Users/shrikanth.mysore/Downloads/AvailabilityData/SavedResults/"
  val currencyConverterFIleLocation = "/Users/shrikanth.mysore/Downloads/TPData/currencyConvertedTable.csv"
  val EStreamingLogFileLocation = "/Users/shrikanth.mysore/Downloads/TPData/eStreaming15S3/*.parquet"

  val spark = SparkConfiguration.sparkSqlConfig()
  import spark.implicits._

  val rdf = new ReadData

  val currencyConverter = rdf.readTPcurrencyCSV(spark, currencyConverterFIleLocation)
  currencyConverter.show(10,false)

  val logEStrParquetDF = rdf.readParquet(spark, EStreamingLogFileLocation)
  logEStrParquetDF.printSchema()
  logEStrParquetDF.show(2,false)

  val midtDF = rdf.readParquet(spark, midtData)
  midtDF.printSchema()
  midtDF.show(2, false)

 // val esdf = new eStreamingData

  val shortListAttributes = eStreamingData.collectAirportAirline(spark, logEStrParquetDF)
  shortListAttributes.distinct().show(10,false)


  spark.stop()
  val duration = (System.nanoTime - t1) / 1e9d
  println("\n\ntotal Execution Time : " + duration + " in seconds\n\n")
}

/*
  /opt/homebrew/bin/spark-submit /Users/shrikanth.mysore/Documents/GitProjects/VirtualInterline/target/scala-2.12/virtualinterline_2.12-0.1.0-SNAPSHOT.jar
 /opt/homebrew/bin/spark-submit /Users/shrikanth.mysore/Documents/GitProjects/VirtualInterline/target/scala-3.3.1/virtualinterline_3-0.1.0-SNAPSHOT.jar

 */
/*
Currency Converted to USD
|Currency|convertedToUSD|

 */
/* eStreaming 1.5  data

 |-- id: long (nullable = true)
 |-- group_id: integer (nullable = true)
 |-- originalRequest: string (nullable = true)
 |-- shop_req_timeStamp: long (nullable = true)
 |-- gds: string (nullable = true)
 |-- pcc: string (nullable = true)
 |-- point_of_sale: string (nullable = true)
 |-- constricted_search: boolean (nullable = true)
 |-- split_ticket: boolean (nullable = true)
 |-- lowest_fare_not_in_result: boolean (nullable = true)
 |-- request_PTC: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- robotic_shop_type: integer (nullable = true)
 |-- confidence_level: string (nullable = true)
 |-- pos_city: string (nullable = true)
 |-- private_fare: boolean (nullable = true)
 |-- currency: string (nullable = true)
 |-- fare: double (nullable = true)
 |-- tax: double (nullable = true)
 |-- validating_cxr: string (nullable = true)
 |-- fare_break_down_by_PTC: array (nullable = true)
 |    |-- element: double (containsNull = true)
 |-- tax_break_down_by_PTC: array (nullable = true)
 |    |-- element: double (containsNull = true)
 |-- tax_breakdown: array (nullable = true)
 |    |-- element: struct (containsNull = true)
 |    |    |-- tax_code: string (nullable = true)
 |    |    |-- tax_amount: double (nullable = true)
 |-- refund_rule: string (nullable = true)
 |-- response_PTC: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- channel_id: string (nullable = true)
 |-- private_fare_account_code: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- fare_construction_text: string (nullable = true)
 |-- out_durations: array (nullable = true)
 |    |-- element: integer (containsNull = true)
 |-- out_trip_duration_time: integer (nullable = true)
 |-- out_layovers: array (nullable = true)
 |    |-- element: integer (containsNull = true)
 |-- out_origin_airport: string (nullable = true)
 |-- out_destination_airport: string (nullable = true)
 |-- out_origin_city: string (nullable = true)
 |-- out_destination_city: string (nullable = true)
 |-- out_departure_date: long (nullable = true)
 |-- out_departure_time: integer (nullable = true)
 |-- out_arrival_date: long (nullable = true)
 |-- out_arrival_time: integer (nullable = true)
 |-- out_departure_time_zone: string (nullable = true)
 |-- out_arrival_time_zone: string (nullable = true)
 |-- out_departure_epoc: long (nullable = true)
 |-- out_arrival_epoc: long (nullable = true)
 |-- out_departure_terminal: string (nullable = true)
 |-- out_arrival_terminal: string (nullable = true)
 |-- out_airport_search: boolean (nullable = true)
 |-- out_marketing_cxr: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- out_operating_cxr: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- out_flight_numbers: array (nullable = true)
 |    |-- element: integer (containsNull = true)
 |-- out_booking_class: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- out_cabin_class: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- out_seats: array (nullable = true)
 |    |-- element: integer (containsNull = true)
 |-- out_avail_source: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- out_equipments: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- out_segments: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- out_fare_components: array (nullable = true)
 |    |-- element: array (containsNull = true)
 |    |    |-- element: array (containsNull = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- fare_basis_code: string (nullable = true)
 |    |    |    |    |-- ticket_designator: string (nullable = true)
 |-- out_fare_types: array (nullable = true)
 |    |-- element: array (containsNull = true)
 |    |    |-- element: string (containsNull = true)
 |-- out_baggage: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- out_refund_rule: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- out_incidental_stops: array (nullable = true)
 |    |-- element: integer (containsNull = true)
 |-- out_incidental_stop_airport: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- out_incidental_stop_departure: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- out_incidental_stop_arrival: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- out_change_of_gauge: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- out_stop_over: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- out_availability_connection_indicator: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- out_remaining_seats: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- out_brand_id: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- out_codeshare_operating_carrier: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- out_codeshare_operating_flight: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- out_via_airports: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- out_num_stops: integer (nullable = true)
 |-- in_durations: array (nullable = true)
 |    |-- element: integer (containsNull = true)
 |-- in_trip_duration_time: integer (nullable = true)
 |-- in_layovers: array (nullable = true)
 |    |-- element: integer (containsNull = true)
 |-- in_origin_airport: string (nullable = true)
 |-- in_destination_airport: string (nullable = true)
 |-- in_origin_city: string (nullable = true)
 |-- in_destination_city: string (nullable = true)
 |-- in_departure_date: long (nullable = true)
 |-- in_departure_time: integer (nullable = true)
 |-- in_arrival_date: long (nullable = true)
 |-- in_arrival_time: integer (nullable = true)
 |-- in_departure_time_zone: string (nullable = true)
 |-- in_arrival_time_zone: string (nullable = true)
 |-- in_departure_epoc: long (nullable = true)
 |-- in_arrival_epoc: long (nullable = true)
 |-- in_departure_terminal: string (nullable = true)
 |-- in_arrival_terminal: string (nullable = true)
 |-- in_airport_search: boolean (nullable = true)
 |-- in_marketing_cxr: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- in_operating_cxr: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- in_flight_numbers: array (nullable = true)
 |    |-- element: integer (containsNull = true)
 |-- in_booking_class: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- in_cabin_class: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- in_seats: array (nullable = true)
 |    |-- element: integer (containsNull = true)
 |-- in_avail_source: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- in_equipments: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- in_segments: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- in_fare_components: array (nullable = true)
 |    |-- element: array (containsNull = true)
 |    |    |-- element: array (containsNull = true)
 |    |    |    |-- element: struct (containsNull = true)
 |    |    |    |    |-- fare_basis_code: string (nullable = true)
 |    |    |    |    |-- ticket_designator: string (nullable = true)
 |-- in_fare_types: array (nullable = true)
 |    |-- element: array (containsNull = true)
 |    |    |-- element: string (containsNull = true)
 |-- in_baggage: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- in_refund_rule: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- in_incidental_stops: array (nullable = true)
 |    |-- element: integer (containsNull = true)
 |-- in_incidental_stop_airport: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- in_incidental_stop_departure: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- in_incidental_stop_arrival: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- in_change_of_gauge: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- in_stop_over: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- in_availability_connection_indicator: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- in_remaining_seats: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- in_brand_id: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- in_codeshare_operating_carrier: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- in_codeshare_operating_flight: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- in_via_airports: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- in_num_stops: integer (nullable = true)


 */
/* MIDT Itinerary


 |-- sat_ngt_stay: string (nullable = true)
 |-- stay_dur: string (nullable = true)
 |-- pos_ctry_cd: string (nullable = true)
 |-- trip_op_al: string (nullable = true)
 |-- agency: string (nullable = true)
 |-- agency_chk: string (nullable = true)
 |-- pcc: string (nullable = true)
 |-- trans_date: string (nullable = true)
 |-- proc_date: string (nullable = true)
 |-- crs: string (nullable = true)
 |-- pnr: string (nullable = true)
 |-- nd_org: string (nullable = true)
 |-- nd_dst: string (nullable = true)
 |-- org: string (nullable = true)
 |-- dst: string (nullable = true)
 |-- trip_al: string (nullable = true)
 |-- trip_class: string (nullable = true)
 |-- trip_pax: long (nullable = true)
 |-- dom_int_cd: string (nullable = true)
 |-- passive_ind: string (nullable = true)
 |-- poa: string (nullable = true)
 |-- equipment: string (nullable = true)
 |-- equip_type: string (nullable = true)
 |-- elapsed_time: string (nullable = true)
 |-- mileage: long (nullable = true)
 |-- total_stops: long (nullable = true)
 |-- org_wac: string (nullable = true)
 |-- dst_wac: string (nullable = true)
 |-- dep_ctry_cd: string (nullable = true)
 |-- arr_ctry_cd: string (nullable = true)
 |-- org_metro_cd: string (nullable = true)
 |-- dst_metro_cd: string (nullable = true)
 |-- dep_date: string (nullable = true)
 |-- dep_time: string (nullable = true)
 |-- dep_day: string (nullable = true)
 |-- arr_date: string (nullable = true)
 |-- arr_time: string (nullable = true)
 |-- arr_day: string (nullable = true)
 |-- gmt_dep_date: string (nullable = true)
 |-- gmt_dep_time: string (nullable = true)
 |-- gmt_arr_date: string (nullable = true)
 |-- gmt_arr_time: string (nullable = true)
 |-- int_ind: string (nullable = true)
 |-- compartment: string (nullable = true)
 |-- adv_pur_days: long (nullable = true)
 |-- seg_cnt: long (nullable = true)
 |-- seg_org_1: string (nullable = true)
 |-- seg_dep_date_1: string (nullable = true)
 |-- seg_dep_time_1: string (nullable = true)
 |-- seg_gmt_dep_date_1: string (nullable = true)
 |-- seg_gmt_dep_time_1: string (nullable = true)
 |-- seg_org_ctry_cd_1: string (nullable = true)
 |-- seg_org_wac_ctry_cd_1: string (nullable = true)
 |-- seg_dep_day_1: string (nullable = true)
 |-- seg_dst_1: string (nullable = true)
 |-- seg_arr_date_1: string (nullable = true)
 |-- seg_arr_time_1: string (nullable = true)
 |-- seg_arr_gmt_date_1: string (nullable = true)
 |-- seg_arr_gmt_time_1: string (nullable = true)
 |-- seg_dst_ctry_cd_1: string (nullable = true)
 |-- seg_dst_wac_ctry_cd_1: string (nullable = true)
 |-- seg_al_cd_1: string (nullable = true)
 |-- seg_flt_num_1: string (nullable = true)
 |-- seg_bkg_cls_1: string (nullable = true)
 |-- seg_equip_1: string (nullable = true)
 |-- seg_equip_type_1: string (nullable = true)
 |-- seg_mileage_1: string (nullable = true)
 |-- seg_stops_1: string (nullable = true)
 |-- seg_elapsed_time_1: string (nullable = true)
 |-- seg_op_al_1: string (nullable = true)
 |-- seg_flt_stp_apt_1_1: string (nullable = true)
 |-- seg_flt_stp_apt_2_1: string (nullable = true)
 |-- seg_flt_stp_apt_3_1: string (nullable = true)
 |-- seg_final_status_1: string (nullable = true)
 |-- seg_al_owner_1: string (nullable = true)
 |-- seg_org_2: string (nullable = true)
 |-- seg_dep_date_2: string (nullable = true)
 |-- seg_dep_time_2: string (nullable = true)
 |-- seg_gmt_dep_date_2: string (nullable = true)
 |-- seg_gmt_dep_time_2: string (nullable = true)
 |-- seg_org_ctry_cd_2: string (nullable = true)
 |-- seg_org_wac_ctry_cd_2: string (nullable = true)
 |-- seg_dep_day_2: string (nullable = true)
 |-- seg_dst_2: string (nullable = true)
 |-- seg_arr_date_2: string (nullable = true)
 |-- seg_arr_time_2: string (nullable = true)
 |-- seg_arr_gmt_date_2: string (nullable = true)
 |-- seg_arr_gmt_time_2: string (nullable = true)
 |-- seg_dst_ctry_cd_2: string (nullable = true)
 |-- seg_dst_wac_ctry_cd_2: string (nullable = true)
 |-- seg_al_cd_2: string (nullable = true)
 |-- seg_flt_num_2: string (nullable = true)
 |-- seg_bkg_cls_2: string (nullable = true)
 |-- seg_equip_2: string (nullable = true)
 |-- seg_equip_type_2: string (nullable = true)
 |-- seg_mileage_2: string (nullable = true)
 |-- seg_stops_2: string (nullable = true)
 |-- seg_elapsed_time_2: string (nullable = true)
 |-- seg_op_al_2: string (nullable = true)
 |-- seg_flt_stp_apt_1_2: string (nullable = true)
 |-- seg_flt_stp_apt_2_2: string (nullable = true)
 |-- seg_flt_stp_apt_3_2: string (nullable = true)
 |-- seg_final_status_2: string (nullable = true)
 |-- seg_al_owner_2: string (nullable = true)
 |-- seg_org_3: string (nullable = true)
 |-- seg_dep_date_3: string (nullable = true)
 |-- seg_dep_time_3: string (nullable = true)
 |-- seg_gmt_dep_date_3: string (nullable = true)
 |-- seg_gmt_dep_time_3: string (nullable = true)
 |-- seg_org_ctry_cd_3: string (nullable = true)
 |-- seg_org_wac_ctry_cd_3: string (nullable = true)
 |-- seg_dep_day_3: string (nullable = true)
 |-- seg_dst_3: string (nullable = true)
 |-- seg_arr_date_3: string (nullable = true)
 |-- seg_arr_time_3: string (nullable = true)
 |-- seg_arr_gmt_date_3: string (nullable = true)
 |-- seg_arr_gmt_time_3: string (nullable = true)
 |-- seg_dst_ctry_cd_3: string (nullable = true)
 |-- seg_dst_wac_ctry_cd_3: string (nullable = true)
 |-- seg_al_cd_3: string (nullable = true)
 |-- seg_flt_num_3: string (nullable = true)
 |-- seg_bkg_cls_3: string (nullable = true)
 |-- seg_equip_3: string (nullable = true)
 |-- seg_equip_type_3: string (nullable = true)
 |-- seg_mileage_3: string (nullable = true)
 |-- seg_stops_3: string (nullable = true)
 |-- seg_elapsed_time_3: string (nullable = true)
 |-- seg_op_al_3: string (nullable = true)
 |-- seg_flt_stp_apt_1_3: string (nullable = true)
 |-- seg_flt_stp_apt_2_3: string (nullable = true)
 |-- seg_flt_stp_apt_3_3: string (nullable = true)
 |-- seg_final_status_3: string (nullable = true)
 |-- seg_al_owner_3: string (nullable = true)
 |-- seg_org_4: string (nullable = true)
 |-- seg_dep_date_4: string (nullable = true)
 |-- seg_dep_time_4: string (nullable = true)
 |-- seg_gmt_dep_date_4: string (nullable = true)
 |-- seg_gmt_dep_time_4: string (nullable = true)
 |-- seg_org_ctry_cd_4: string (nullable = true)
 |-- seg_org_wac_ctry_cd_4: string (nullable = true)
 |-- seg_dep_day_4: string (nullable = true)
 |-- seg_dst_4: string (nullable = true)
 |-- seg_arr_date_4: string (nullable = true)
 |-- seg_arr_time_4: string (nullable = true)
 |-- seg_arr_gmt_date_4: string (nullable = true)
 |-- seg_arr_gmt_time_4: string (nullable = true)
 |-- seg_dst_ctry_cd_4: string (nullable = true)
 |-- seg_dst_wac_ctry_cd_4: string (nullable = true)
 |-- seg_al_cd_4: string (nullable = true)
 |-- seg_flt_num_4: string (nullable = true)
 |-- seg_bkg_cls_4: string (nullable = true)
 |-- seg_equip_4: string (nullable = true)
 |-- seg_equip_type_4: string (nullable = true)
 |-- seg_mileage_4: string (nullable = true)
 |-- seg_stops_4: string (nullable = true)
 |-- seg_elapsed_time_4: string (nullable = true)
 |-- seg_op_al_4: string (nullable = true)
 |-- seg_flt_stp_apt_1_4: string (nullable = true)
 |-- seg_flt_stp_apt_2_4: string (nullable = true)
 |-- seg_flt_stp_apt_3_4: string (nullable = true)
 |-- seg_final_status_4: string (nullable = true)
 |-- seg_al_owner_4: string (nullable = true)
 |-- seg_org_5: string (nullable = true)
 |-- seg_dep_date_5: string (nullable = true)
 |-- seg_dep_time_5: string (nullable = true)
 |-- seg_gmt_dep_date_5: string (nullable = true)
 |-- seg_gmt_dep_time_5: string (nullable = true)
 |-- seg_org_ctry_cd_5: string (nullable = true)
 |-- seg_org_wac_ctry_cd_5: string (nullable = true)
 |-- seg_dep_day_5: string (nullable = true)
 |-- seg_dst_5: string (nullable = true)
 |-- seg_arr_date_5: string (nullable = true)
 |-- seg_arr_time_5: string (nullable = true)
 |-- seg_arr_gmt_date_5: string (nullable = true)
 |-- seg_arr_gmt_time_5: string (nullable = true)
 |-- seg_dst_ctry_cd_5: string (nullable = true)
 |-- seg_dst_wac_ctry_cd_5: string (nullable = true)
 |-- seg_al_cd_5: string (nullable = true)
 |-- seg_flt_num_5: string (nullable = true)
 |-- seg_bkg_cls_5: string (nullable = true)
 |-- seg_equip_5: string (nullable = true)
 |-- seg_equip_type_5: string (nullable = true)
 |-- seg_mileage_5: string (nullable = true)
 |-- seg_stops_5: string (nullable = true)
 |-- seg_elapsed_time_5: string (nullable = true)
 |-- seg_op_al_5: string (nullable = true)
 |-- seg_flt_stp_apt_1_5: string (nullable = true)
 |-- seg_flt_stp_apt_2_5: string (nullable = true)
 |-- seg_flt_stp_apt_3_5: string (nullable = true)
 |-- seg_final_status_5: string (nullable = true)
 |-- seg_al_owner_5: string (nullable = true)
 |-- seg_org_6: string (nullable = true)
 |-- seg_dep_date_6: string (nullable = true)
 |-- seg_dep_time_6: string (nullable = true)
 |-- seg_gmt_dep_date_6: string (nullable = true)
 |-- seg_gmt_dep_time_6: string (nullable = true)
 |-- seg_org_ctry_cd_6: string (nullable = true)
 |-- seg_org_wac_ctry_cd_6: string (nullable = true)
 |-- seg_dep_day_6: string (nullable = true)
 |-- seg_dst_6: string (nullable = true)
 |-- seg_arr_date_6: string (nullable = true)
 |-- seg_arr_time_6: string (nullable = true)
 |-- seg_arr_gmt_date_6: string (nullable = true)
 |-- seg_arr_gmt_time_6: string (nullable = true)
 |-- seg_dst_ctry_cd_6: string (nullable = true)
 |-- seg_dst_wac_ctry_cd_6: string (nullable = true)
 |-- seg_al_cd_6: string (nullable = true)
 |-- seg_flt_num_6: string (nullable = true)
 |-- seg_bkg_cls_6: string (nullable = true)
 |-- seg_equip_6: string (nullable = true)
 |-- seg_equip_type_6: string (nullable = true)
 |-- seg_mileage_6: string (nullable = true)
 |-- seg_stops_6: string (nullable = true)
 |-- seg_elapsed_time_6: string (nullable = true)
 |-- seg_op_al_6: string (nullable = true)
 |-- seg_flt_stp_apt_1_6: string (nullable = true)
 |-- seg_flt_stp_apt_2_6: string (nullable = true)
 |-- seg_flt_stp_apt_3_6: string (nullable = true)
 |-- seg_final_status_6: string (nullable = true)
 |-- seg_al_owner_6: string (nullable = true)
 |-- monthly: long (nullable = true)
 |-- is_abk: long (nullable = true)
 |-- type: string (nullable = true)
 |-- proc_dd: long (nullable = true)

 */
