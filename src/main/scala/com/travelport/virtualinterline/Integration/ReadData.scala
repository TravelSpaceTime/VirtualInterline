package com.travelport.virtualinterline.Integration

import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

class ReadData {

  def readParquet(spark: SparkSession, logFileLocation: String): DataFrame = {
    val logData = spark.read.parquet(logFileLocation).toDF
    logData
  }

  def readOXcurrencyCSV(spark: SparkSession, csvFileLocation: String): DataFrame = {
    val schema = new StructType()
      .add("Currency", StringType, true)
      .add("OneMillionCurrencyInUSD", DoubleType, true)

    val df_with_schema = spark.read.format("csv")
      .option("header", "false")
      .schema(schema)
      .load(csvFileLocation)
    df_with_schema
  }

    def readTPcurrencyCSV(spark: SparkSession, csvFileLocation: String): DataFrame = {
    val schema = new StructType()
      .add("Currency", StringType, true)
      .add("convertedToUSD", DoubleType, true)

    val df_with_schema = spark.read.format("csv")
      .option("header", "true")
      .schema(schema)
      .load(csvFileLocation)
    df_with_schema
  }
  def readMIDTcsv(spark: SparkSession, csvFileLocation: String): DataFrame = {
    val schema = new StructType()
    /*
    "pnrcreateyyyymmdd","org","dst","seg_org_1","seg_dst_1","seg_org_2","seg_dst_2","seg_org_3","seg_dst_3","seg_org_4","seg_dst_4","seg_org_5","seg_dst_5","seg_org_6","seg_dst_6","seg_al_cd_1","seg_al_cd_2","seg_al_cd_3","seg_al_cd_4","seg_al_cd_5","seg_al_cd_6","bkgcnt"
    "20231004","AAE","ALG","AAE","ALG","","","","","","","","","","","AH","","","","","","167"
     */
      .add("pnrcreateyyyymmdd", LongType, true)
      .add("org", StringType, true)
      .add("dst", StringType, true)
      .add("seg_org_1", StringType, true)
      .add("seg_dst_1", StringType, true)
      .add("seg_org_2", StringType, true)
      .add("seg_dst_2", StringType, true)
      .add("seg_org_3", StringType, true)
      .add("seg_dst_3", StringType, true)
      .add("seg_org_4", StringType, true)
      .add("seg_dst_4", StringType, true)
      .add("seg_org_5", StringType, true)
      .add("seg_dst_5", StringType, true)
      .add("seg_org_6", StringType, true)
      .add("seg_dst_6", StringType, true)
      .add("seg_al_cd_1", StringType, true)
      .add("seg_al_cd_2", StringType, true)
      .add("seg_al_cd_3", StringType, true)
      .add("seg_al_cd_4", StringType, true)
      .add("seg_al_cd_5", StringType, true)
      .add("seg_al_cd_6", StringType, true)
      .add("bkgcnt", IntegerType, true)

    val df_with_schema = spark.read.format("csv")
      .option("header", "true")
      .schema(schema)
      .load(csvFileLocation)
    df_with_schema
  }

  def readSingleLineJson(spark: SparkSession, logFileLocation: String): DataFrame = {

    val logData = spark.read.json(logFileLocation).toDF.cache()
    logData
  }

  def readMultiLineJson(spark: SparkSession, logFileLocation: String): DataFrame = {

    val logData = spark.read.option("multiline", "true").json(logFileLocation).toDF.cache()
    logData
  }



  ///s3/buckets/tvlp-ds-midt-connection-points-pn?region=us-east-1&prefix=year%3D2023/month%3D10/&showversions=false

}
