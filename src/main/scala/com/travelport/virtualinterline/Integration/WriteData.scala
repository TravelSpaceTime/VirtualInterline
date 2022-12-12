package com.travelport.virtualinterline.Integration

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object WriteData {
  def writeCsvFromDF(spark: SparkSession,dfToWrite: DataFrame,csvSaveLocation:String):Unit = {
    //import spark.implicits._
    dfToWrite.write.mode(SaveMode.Overwrite).option("header",true).format("csv").csv(csvSaveLocation)
  }

  def writeparquetFromDF(spark: SparkSession,dfToWrite: DataFrame,parquetSaveLocation:String):Unit = {

    dfToWrite.write.mode(SaveMode.Overwrite).option("compression","snappy").parquet(parquetSaveLocation)
  }

}
