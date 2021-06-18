

import java.sql.{Connection, DriverManager, Statement}
import java.util.Properties

import org.apache.commons.codec.StringDecoder
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.clients
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.dstream.InputDStream
import spray.json.{JsValue, JsonParser}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks


object sparkSteamDev {
  def main(args: Array[String]): Unit = {
    val sparkConf  = new SparkConf().setMaster("local[*]").setAppName("sparkStream")
    val streamingContext = new StreamingContext(sparkConf,Seconds(5))
    streamingContext.checkpoint("./saveCheckPoint")


    val prop = new Properties()
    val inputStream = getClass.getClassLoader.getResourceAsStream("application.properties")
    prop.load(inputStream);
    val sparkMaster = prop.get("kafkaParams")


    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.207.136:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test1"
      //"max.poll.records" -> "100"
    )

    val topics = Set("launch")
    val topicPartition = new TopicPartition("launch", 1)
    val offset:mutable.Map[TopicPartition, Long] = mutable.Map()
    offset += (topicPartition->0L)

    //val kafkaDStream:InputDStream[(String, String)] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, fromOffsets, messageHandler) //1.6.3版本 已废弃
    val kafkaDStream = KafkaUtils.createDirectStream(streamingContext,LocationStrategies.PreferConsistent,Subscribe[String,String](topics, kafkaParams))
    //var wordStream = kafkaDStream.flatMap(x=>x.value.split(" ")).map((_,1)).reduceByKey(_+_)

    //连接
    //val connection: Connection = DriverManager.getConnection("jdbc:clickhouse://192.168.207.136:8123", "default", "")
    //val statement: Statement = connection.createStatement()

    //查找条件优先级 imei->oaid->android_id->mac->ip
    val sqls = mutable.LinkedHashMap[String,String](
      /*"imei"->"SELECT  * FROM test.array_test WHERE imei_md5=?",
      "oaid"->"SELECT  * FROM test.array_test WHERE oaid=?",
      "androidid"->"SELECT  * FROM test.array_test WHERE androidid_md5=?",
      "mac"->"SELECT  * FROM test.array_test WHERE mac_md5=?",
      "ip"->"SELECT  * FROM test.array_test WHERE ip=?"*/
      "imei"->"SELECT  * FROM test.array_test WHERE imei_md5=?"
    )

    val deviceInfoStream = ArrayBuffer[String]()
    //println(sqls)
    var wordStream = kafkaDStream.foreachRDD(x=>{
        x.foreach(device=>{
          val deviceJsObj  = JsonParser(device.value).asJsObject()
          deviceInfoStream += (device.value)
          Class.forName("ru.yandex.clickhouse.ClickHouseDriver")
          //Class.forName("net.sf.log4jdbc.DriverSpy")
          val connection: Connection = DriverManager.getConnection("jdbc:clickhouse://192.168.207.136:8123", "default", "")
          //查找7天内的clickhouse数据进行归因
          val loop = new Breaks;
          loop.breakable {
            for ((k, sql) <- sqls) {
              //println(sql)

              val prep = connection.prepareStatement(sql)
              k match {
                case "imei"=>prep.setString(1, deviceJsObj.getFields("imei").head.toString().replace("\"", ""))
                case "oaid"=>prep.setString(1, deviceJsObj.getFields("oaid").head.toString())
                case "androidid"=>prep.setString(1, deviceJsObj.getFields("androidid").head.toString())
                case "mac"=>prep.setString(1, deviceJsObj.getFields("mac").head.toString())
                case "ip"=>prep.setString(1, deviceJsObj.getFields("ip").head.toString())
              }

              val res = prep.executeQuery
              //广告归因信息
              val advAscribeInfo:mutable.Map[String,String] = mutable.Map[String,String]("plan_id"->"0", "channel_id"->"0")
              //println(prep)
              println(res)
              while (res.next()) {
                advAscribeInfo("plan_id") = res.getString("plan_id")
                advAscribeInfo("channel_id") = res.getString("channel_id")
                println("find it")
                loop.break()
              }

            }
          }

        }
      )
    })
    //println(deviceInfoStream)

    //wordStream = wordStream.updateStateByKey(updateFunc)
    //wordStream.print(1)
    streamingContext.start()
    streamingContext.awaitTermination()

  }

  def updateFunc(values:Seq[Int],state:Option[Int]):Option[Int] = {
    val _old = state.getOrElse(0)
    val _new = values.sum
    Some(_old + _new)
  }

}
