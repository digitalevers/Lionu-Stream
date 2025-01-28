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
    // 定义JSON Schema
    val jsonSchema = new StructType()
      .add("name", StringType)
      .add("age", IntegerType)
      .add("city", StringType)

    // 解析JSON数据为 StructType
    val parsedDF = kafkaDF.selectExpr("CAST(value AS STRING) AS json").select(from_json(col("json"), jsonSchema).as("parsed_json"))

    // 处理每一批次的数据
    val query = parsedDF.writeStream.outputMode("append").foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        batchDF.foreach { row =>
          val parsedJson = row.getAs[org.apache.spark.sql.Row]("parsed_json")
          if (parsedJson != null) {
            val map = parsedJson.getValuesMap[String](parsedJson.schema.fieldNames)
            println(s"Decoded Map: $map")
            // 在这里可以对 map 进行进一步处理
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
