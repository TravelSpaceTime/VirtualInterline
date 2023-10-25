package com.travelport.virtualinterline.Analysis

import org.apache.spark.sql.{DataFrame, SparkSession}

class eStreamingData {
  /*

|id              |group_id|originalRequest             |shop_req_timeStamp
|gds|pcc |point_of_sale|constricted_search|split_ticket|lowest_fare_not_in_result|request_PTC|robotic_shop_type
|confidence_level|pos_city|private_fare|currency|fare   |tax   |validating_cxr|fare_break_down_by_PTC|tax_break_down_by_PTC
|tax_breakdown                                                                                         |refund_rule                            |response_PTC|channel_id|private_fare_account_code
|fare_construction_text                                                                |out_durations |out_trip_duration_time
|out_layovers|out_origin_airport|out_destination_airport|out_origin_city|out_destination_city|out_departure_date
|out_departure_time|out_arrival_date|out_arrival_time|out_departure_time_zone|out_arrival_time_zone|out_departure_epoc
|out_arrival_epoc|out_departure_terminal|out_arrival_terminal|out_airport_search|out_marketing_cxr|out_operating_cxr
|out_flight_numbers|out_booking_class|out_cabin_class|out_seats|out_avail_source|out_equipments |out_segments|out_fare_components
|out_fare_types|out_baggage|out_refund_rule|out_incidental_stops|out_incidental_stop_airport|out_incidental_stop_departure
|out_incidental_stop_arrival|out_change_of_gauge|out_stop_over|out_availability_connection_indicator|out_remaining_seats
|out_brand_id|out_codeshare_operating_carrier|out_codeshare_operating_flight|out_via_airports|out_num_stops
|in_durations|in_trip_duration_time|in_layovers|in_origin_airport|in_destination_airport|in_origin_city|in_destination_city
|in_departure_date|in_departure_time|in_arrival_date|in_arrival_time|in_departure_time_zone|in_arrival_time_zone|in_departure_epoc
|in_arrival_epoc|in_departure_terminal|in_arrival_terminal|in_airport_search|in_marketing_cxr|in_operating_cxr|in_flight_numbers
|in_booking_class|in_cabin_class|in_seats|in_avail_source|in_equipments|in_segments|in_fare_components
|in_fare_types|in_baggage|in_refund_rule|in_incidental_stops|in_incidental_stop_airport
|in_incidental_stop_departure|in_incidental_stop_arrival|in_change_of_gauge|in_stop_over|in_availability_connection_indicator
|in_remaining_seats|in_brand_id|in_codeshare_operating_carrier|in_codeshare_operating_flight|in_via_airports|in_num_stops|

   */
  def collectOutAirportAirline(spark: SparkSession, logData:DataFrame):DataFrame={
    import spark.implicits._
    val shortlistAttributes = logData.select(
      "out_origin_city"
       ,"out_via_airports","out_destination_city"
      //,"out_marketing_cxr","out_operating_cxr"
      //,"out_num_stops","in_origin_airport","in_destination_airport"
      //, "in_marketing_cxr","in_operating_cxr","in_via_airports","in_num_stops"

    )
    


    shortlistAttributes
  }

}
