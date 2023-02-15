package com.travelport.virtualinterline.Integration

import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadData {

  def readParquet(spark: SparkSession, logFileLocation: String): DataFrame = {
    val logData = spark.read.parquet(logFileLocation).toDF
    logData
  }

}
