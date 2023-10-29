package com.travelport.virtualinterline.Integration

import com.travelport.virtualinterline.Analysis.eStreamingData
import org.apache.spark.sql.SparkSession

object SparkConfiguration {
  def sparkSqlConfig(): SparkSession = {
    val spark = SparkSession.builder.appName("AvailabilityPredictor")
      .master("local[4]")
      .config("spark.executor.memory", "8g")
      .config("spark.driver.memory", "8g")
      .config("spark.executor.memoryOverhead", "8g")
      .config("spark.driver.memoryOverhead", "8g")
      .config("spark.memory.offHeap.enabled", "true")
      .config("spark.memory.offHeap.size", "8g")
      .config("spark.kryoserializer.buffer.max", "1000m")
      .config("spark.kryoserializer.buffer", "200m")
      .config("spark.executor.extraJavaOptions", "-XX:MaxPermSize=4G -XX:+UseG1GC")
      .config("spark.driver.extraJavaOptions", "-XX:MaxPermSize=8G -Xms=8G -Xmx=8G")
      .config("spark.memory.fraction", "0.4")
      .config("spark.shuffle.memoryFraction","0.4")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
      .getOrCreate()

    val conf = spark.sparkContext
    conf.getConf.registerKryoClasses(Array(classOf[ReadData]))
    conf.setLogLevel("ERROR")

    spark
  }


}
