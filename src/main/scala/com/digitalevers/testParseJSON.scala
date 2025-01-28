package com.digitalevers

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object testParseJSON {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Nested JSON Parsing").getOrCreate()

    import spark.implicits._
    val jsonData = Seq(
      """{"appid": "49", "planid": "", "os": "1", "channel": "default", "deviceInfo": {"applicationId": "com.example.test_library", "appName": "\u91cfU Android SDK DEMO", "versionCode": "1", "versionName": "1.0", "time": "2025-01-14 19:50:12", "imei": "119538509313199m", "androidid": "cf7b0b3b04d837b6", "oaid": "2a10298fe905dbff", "mac": "02:00:00:00:00:00", "model": "M2007J17C", "sys": "31", "ua": "Dalvik\/2.1.0 (Linux; U; Android 12; M2007J17C Build\/SKQ1.211006.001)", "ip": "192.168.2.100"}}"""
    )

    val df = spark.createDataFrame(jsonData.toDF("json"))

    val deviceInfoSchema = StructType(Array(
      StructField("applicationId", StringType, true),
      StructField("appName", StringType, true),
      StructField("versionCode", StringType, true),
      StructField("versionName", StringType, true),
      StructField("time", StringType, true),
      StructField("imei", StringType, true),
      StructField("androidid", StringType, true),
      StructField("oaid", StringType, true),
      StructField("mac", StringType, true),
      StructField("model", StringType, true),
      StructField("sys", StringType, true),
      StructField("ua", StringType, true),
      StructField("ip", StringType, true)
    ))

    val schema = StructType(Array(
      StructField("appid", StringType, true),
      StructField("planid", StringType, true),
      StructField("os", StringType, true),
      StructField("channel", StringType, true),
      StructField("deviceInfo", deviceInfoSchema, true)
    ))

    val parsedDf = df.withColumn("parsed_json", from_json(col("json"), schema))
      .select("parsed_json.*")

    val finalDf = parsedDf.select(
      col("appid"),
      col("planid"),
      col("os"),
      col("channel"),
      col("deviceInfo.applicationId"),
      col("deviceInfo.appName"),
      col("deviceInfo.versionCode"),
      col("deviceInfo.versionName"),
      col("deviceInfo.time"),
      col("deviceInfo.imei"),
      col("deviceInfo.androidid"),
      col("deviceInfo.oaid"),
      col("deviceInfo.mac"),
      col("deviceInfo.model"),
      col("deviceInfo.sys"),
      col("deviceInfo.ua"),
      col("deviceInfo.ip")
    )

    finalDf.show(false)
  }
}
