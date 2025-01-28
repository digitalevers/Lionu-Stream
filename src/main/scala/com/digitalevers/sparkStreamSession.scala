package com.digitalevers

//导入依赖包 spark-sql
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{MapType, StringType, StructType, IntegerType}

import java.util.Properties

object sparkStreamSession {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("KafkaStream").master("local[*]").getOrCreate()  // 使用本地模式运行
    import spark.implicits._
    //读取配置文件
    val prop = new Properties();
    val in = sparkStreamSession.getClass.getClassLoader.getResourceAsStream("application.properties");
    prop.load(in)
    // 1.激活topic
    val launchKafkaParams = this.getKafkaParams(prop,"launch")
    // 从 Kafka 读取数据
    val kafkaDF = spark.readStream.format("kafka").options(launchKafkaParams).load()
    // 定义 deviceInfo 的 Schema
    val deviceInfoSchema = new StructType()
      .add("applicationId", StringType)
      .add("appName", StringType)
      .add("versionCode", StringType)
      .add("versionName", StringType)
      .add("time", StringType)
      .add("imei", StringType)
      .add("androidid", StringType)
      .add("oaid", StringType)
      .add("mac", StringType)
      .add("model", StringType)
      .add("sys", StringType)
      .add("ua", StringType)
      .add("ip", StringType)

    // 定义根对象的 Schema
    val jsonSchema = new StructType()
      .add("appid", StringType)
      .add("planid", StringType)
      .add("os", StringType)
      .add("channel", StringType)
      .add("deviceInfo", deviceInfoSchema)

    // 定义JSON Schema
    /*val jsonSchema = new StructType()
      .add("androidid", StringType)
      .add("appName", IntegerType)
      .add("appid", StringType)
      .add("applicationId", StringType)
      .add("channel", StringType)
      .add("imei", StringType)
      .add("ip", StringType)
      .add("externalip", StringType)
      .add("mac", StringType)
      .add("model", StringType)
      .add("oaid", StringType)
      .add("os", StringType)
      .add("planid", StringType)
      .add("sys", StringType)
      .add("time", StringType)
      .add("ua", StringType)
      .add("versionCode", StringType)
      .add("versionName", StringType)*/

    // 解析JSON数据为 StructType
    //` val parsedDF = kafkaDF.selectExpr("CAST(value AS STRING) AS json").select(from_json(col("json"), jsonSchema).as("parsed_json"))
    val parsedDf = kafkaDF.withColumn("parsed_json", from_json(col("json"), jsonSchema)).select("parsed_json.*")
    val finalDf = parsedDf.select(
      col("field1"),
      col("nestedField.subField1"),
      col("nestedField.subField2")
    )
    // 处理每一批次的数据
    val query = parsedDf.writeStream.outputMode("append").foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        batchDF.foreach { row =>
          val parsedJson = row.getAs[org.apache.spark.sql.Row]("parsed_json")
          if (parsedJson != null) {
            //val map = parsedJson.getValuesMap[String](parsedJson.schema.fieldNames)

            //val map = flattenRow(parsedJson)
            //val deviceInfo = map.get("deviceInfo.ua")
            //println(s"Decoded Map: $deviceInfo")

            // 在这里可以对 map 进行进一步处理
            /*val deviceInfoRow = parsedJson.getAs[org.apache.spark.sql.Row]("deviceInfo")
            if (deviceInfoRow != null) {
              val deviceInfoMap = deviceInfoRow.getValuesMap[String](deviceInfoRow.schema.fieldNames)
              println(s"Device Info: $deviceInfoMap")
              // 在这里可以对 deviceInfoMap 进行进一步处理
            } else {
              println("deviceInfo is null")
            }*/

          } else {
            println("Failed to parse JSON: " + row.getAs[String]("json"))
          }
        }
      }.trigger(Trigger.ProcessingTime("5 seconds")).start()

    // 输出结果到控制台
    //val query = wordCountsDF.writeStream.outputMode("complete").format("console").start()
    query.awaitTermination()
  }

  private def getKafkaParams(_prop:Properties, _topic:String) = {
    val _map =  Map[String, String](
      "kafka.bootstrap.servers" -> _prop.getProperty("kafkaParams.bootstrap.servers"),
      //"key.deserializer" -> classOf[StringDeserializer],
      //"value.deserializer" -> classOf[StringDeserializer],
      "subscribe" -> _topic
    )
    _map
  }

  // 递归函数，将 Row 对象展平为 Map
  private def flattenRow(row: org.apache.spark.sql.Row, prefix: String = ""): Map[String, Any] = {
    row.schema.fields.zip(row.toSeq).flatMap { case (field, value) =>
      if (value == null) {
        Map(prefix + field.name -> null)
      } else if (field.dataType.isInstanceOf[StructType]) {
        flattenRow(value.asInstanceOf[org.apache.spark.sql.Row], prefix + field.name + ".")
      } else {
        Map(prefix + field.name -> value)
      }
    }.toMap
  }

//  def main(args: Array[String]): Unit = {
//
//    val ss = SparkSession.builder().appName("KafkaConsumerExample").master("local").getOrCreate()
//
//    // 定义Kafka参数
//    // 创建DataFrame，代表从Kafka读取的数据
//    val df = ss.readStream.format("kafka")
//      .option("kafka.bootstrap.servers", "192.168.2.108:9092")
//      //.option("group.id", "group1")
//      //.option("auto.offset.reset", "earliest")
//      //.option("enable.auto.commit", "false: java.lang.Boolean")
//      .option("subscribe", "digitalevers")
//      .load()
//
//    // 将Kafka中的数据转换为DataFrame
//    /*val dataFrame = df.selectExpr($"key".cast("string"), $"value".cast("string"), $"timestamp".cast("timestamp"),
//      $"topic",
//      $"partition"
//    )*/
//
//    // 对数据进行处理，例如打印
//    val query: StreamingQuery = df
//      .writeStream
//      //.format("console")
//      .outputMode("append")
//      .foreach(new ForeachWriter[Row] {
//        def open(partitionId: Long, version: Long): Boolean = {
//          true
//        }
//
//        def process(record: Row): Unit = {
//          println(s"Key: ${record.getAs[String]("key")}, Value: ${record.getAs[String]("value")}")
//        }
//
//        def close(errorOrNull: Throwable): Unit = {
//          // 关闭资源
//        }
//      })
//      .start()
//    query.awaitTermination()
//  }
}
