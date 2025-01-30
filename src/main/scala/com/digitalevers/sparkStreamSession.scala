package com.digitalevers

//导入依赖包 spark-sql
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.Trigger
import java.util.Properties

//直接使用spark解析json不太理想
//所以先读取kafka中的字符串 然后使用scala来进行json解析
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

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
    val parsedDF = kafkaDF.selectExpr("CAST(value AS STRING) as jsonString")
    // 处理每一批次的数据
    val query = parsedDF.writeStream.outputMode("append").foreachBatch { (batchDF: DataFrame, _: Long) =>
        batchDF.foreach { row =>
          //println(s"Decoded row: $row")
          val mapper = new ObjectMapper()
          mapper.registerModule(DefaultScalaModule)
          try {
            //提取row中的字符串
            val jsonString = row.getAs[String]("jsonString")
            val map: Map[String, Any] = mapper.readValue(jsonString, classOf[Map[String, Any]])
            println(map)
          } catch {
            case e: Exception => println(s"Error decoding JSON: ${e.getMessage}")
          }
        }
      }.trigger(Trigger.ProcessingTime("5 seconds")).start()

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
}
