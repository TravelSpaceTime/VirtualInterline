package com.travelport.virtualinterline.Integration

import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadData {
  /*

+---------------+---------------+---------------+---+---+---+--------+--------+----------+----------+---+------------+------------+-------------+------------------+------------+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---------------------+----------------------+-------------------+--------------------+--------+---------+-------+--------+----------+-----------+------------------+------+--------------+
|transactionDate|transactionHour|transactionTime|gds|pcc|pos|agentSrc|sourceID|vendorCode|vendorType|cxr|flightNumber|deptDateTime|originAirport|destinationAirport|inventorySrc|A  |B  |C  |D  |E  |F  |G  |H  |I  |J  |K  |L  |M  |N  |O  |P  |Q  |R  |S  |T  |U  |V  |W  |X  |Y  |Z  |rawAvailability      |transactionDateTimeStr|transactionDateTime|deptDateTimeFormated|deptYear|deptMonth|deptDay|deptHour|deptMinute|deptWeekday|hoursTillDeparture|market|airline_market|
+---------------+---------------+---------------+---+---+---+--------+--------+----------+----------+---+------------+------------+-------------+------------------+------------+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---------------------+----------------------+-------------------+--------------------+--------+---------+-------+--------+----------+-----------+------------------+------+--------------+
|20220828       |21             |293626         |P  |5H6|CO |        |        |          |          |LA |4103        |202210041450|CTG          |BOG               |P           |0  |7  |0  |0  |0  |0  |7  |7  |0  |0  |7  |7  |7  |7  |7  |7  |7  |0  |7  |0  |0  |7  |7  |7  |7  |0  |0777777777777777     |20220828-212936       |2022-08-28 21:29:36|2022-10-04 14:50:00 |2022    |10       |4      |14      |50        |Tue        |881               |CTGBOG|LA_CTGBOG     |
|20220828       |21             |294207         |P  |RUL|HK |        |        |          |          |PR |468         |202209241405|MNL          |ICN               |P           |0  |9  |9  |7  |9  |0  |0  |9  |3  |9  |9  |9  |9  |9  |9  |0  |9  |0  |9  |9  |9  |9  |9  |9  |9  |2  |997993999999999999992|20220828-212942       |2022-08-28 21:29:42|2022-09-24 14:05:00 |2022    |9        |24     |14      |5         |Sat        |641               |MNLICN|PR_MNLICN     |
+---------------+---------------+---------------+---+---+---+--------+--------+----------+----------+---+------------+------------+-------------+------------------+------------+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---------------------+----------------------+-------------------+--------------------+--------+---------+-------+--------+----------+-----------+------------------+------+--------------+
only showing top 2 rows
+---+---+-------------+------------------+----------------+---------------------+--------------+-----------+----------+-----------+---------------+-------------+-----------------+-----------------+--------------+-----------------+------+------------------------------------------------------------------+----------+-------+---+----------+----+-----------+------------------+------------+-----------------+-----------------+----------------------+-------------+----------+---------+----------+--------------+------------+----------------+----------------+-------------+----------------+---------+------------------+-------------------+-------------+-----------+------------+----------+------------+----------+-----------+---------+
|gds|pcc|outOriginCity|outDestinationCity|outOriginAirport|outDestinationAirport|outJourneyTime|outDeptTime|outArrTime|outNumStops|outOperatingCxr|outCabinClass|s_outBookingClass|outFareBasisCodes|outViaAirports|s_outMarketingCxr|booked|uniq_key                                                          |solutionID|fare   |pos|s_currency|pax |responsePTC|fareBreakDownByPTC|inOriginCity|inDestinationCity|s_inOriginAirport|s_inDestinationAirport|inJourneyTime|inDeptTime|inArrTime|inNumStops|inOperatingCxr|inCabinClass|s_inBookingClass|inFareBasisCodes|inViaAirports|s_inMarketingCxr|fare_norm|inJourneyTime_norm|outJourneyTime_norm|outDept_month|outDept_day|inDept_month|inDept_day|outArr_month|outArr_day|inArr_month|inArr_day|
+---+---+-------------+------------------+----------------+---------------------+--------------+-----------+----------+-----------+---------------+-------------+-----------------+-----------------+--------------+-----------------+------+------------------------------------------------------------------+----------+-------+---+----------+----+-----------+------------------+------------+-----------------+-----------------+----------------------+-------------+----------+---------+----------+--------------+------------+----------------+----------------+-------------+----------------+---------+------------------+-------------------+-------------+-----------+------------+----------+------------+----------+-----------+---------+
|1G |R1J|DEL          |DXB               |DEL             |DXB                  |760           |15.67      |2.67      |2          |AI             |E            |M                |[[MIP, LLOWBMAE]]|BLR|BOM       |AI|AI|AI         |0     |1_1304928987146878976_151912448_1741668198839457_1660984221_2B3Z4G|58        |34541.0|IN |INR       |null|[ADT]      |[34541.0]         |null        |null             |null             |null                  |null         |null      |null     |null      |null          |null        |null            |[]              |null         |null            |2.8791   |null              |3.7073             |September    |Tue        |null        |null      |September   |Wed       |null       |null     |
+---+---+-------------+------------------+----------------+---------------------+--------------+-----------+----------+-----------+---------------+-------------+-----------------+-----------------+--------------+-----------------+------+------------------------------------------------------------------+----------+-------+---+----------+----+-----------+------------------+------------+-----------------+-----------------+----------------------+-------------+----------+---------+----------+--------------+------------+----------------+----------------+-------------+----------------+---------+------------------+-------------------+-------------+-----------+------------+----------+------------+----------+-----------+---------+
only showing top 1 row



val columns = Seq("gds","pcc","outOriginCity","outDestinationCity","outOriginAirport","outDestinationAirport"
,"outJourneyTime","outDeptTime","outArrTime","outNumStops","outOperatingCxr","outCabinClass","s_outBookingClass"
,"outFareBasisCodes","outViaAirports","s_outMarketingCxr","booked","uniq_key
","solutionID","fare   ","pos","s_currency","pax ","responsePTC","fareBreakDownByPTC"
,"inOriginCity","inDestinationCity","s_inOriginAirport","s_inDestinationAirport","inJourneyTime",
"inDeptTime","inArrTime","inNumStops","inOperatingCxr","inCabinClass","s_inBookingClass"
,"inFareBasisCodes","inViaAirports","s_inMarketingCxr","fare_norm","inJourneyTime_norm","outJourneyTime_norm"
,"outDept_month","outDept_day","inDept_month","inDept_day","outArr_month","outArr_day","inArr_month","inArr_day")
   */

  def readParquet(spark: SparkSession, logFileLocation: String): DataFrame = {
    val logData = spark.read.parquet(logFileLocation).toDF
    logData
  }

}
