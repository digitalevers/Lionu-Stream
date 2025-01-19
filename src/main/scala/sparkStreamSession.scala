package com.digitalevers

//导入依赖包 spark-sql
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.StreamingQuery

object sparkStreamSession {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("KafkaStreamingExample").master("local[*]").getOrCreate()  // 使用本地模式运行
    import spark.implicits._

    // 从 Kafka 读取数据
    val kafkaDF = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "192.168.2.108:9092").option("subscribe", "test-topic").load()
    // 解析 Kafka 数据
    val parsedDF = kafkaDF.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    // 进行简单的流处理，例如计算每个单词的出现次数
    val wordsDF = parsedDF.select(explode(split($"value", " ")) as "word")
    val wordCountsDF = wordsDF.groupBy("word").count()
    // 输出结果到控制台
    val query = wordCountsDF.writeStream.outputMode("complete").format("console").start()
    query.awaitTermination()
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


