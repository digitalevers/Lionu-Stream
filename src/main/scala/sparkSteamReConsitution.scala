/**
 * sparkStream重构版
 */

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, Statement}

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.common.TopicPartition
import spray.json.DefaultJsonProtocol.{StringJsonFormat, mapFormat}
import spray.json.JsonParser
import java.net.{SocketException, SocketTimeoutException}
import java.security.MessageDigest
import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.collection.mutable
import scala.util.Try
import scala.util.control.Breaks


object sparkSteamReConsitution {
  private  val  NOW = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date())
  private  val TODAY = new SimpleDateFormat("yyyy-MM-dd").format(new Date())

  def main(args: Array[String]): Unit = {

    val sparkConf  = new SparkConf().setMaster("local[*]").setAppName("sparkStream")
    val streamingContext = new StreamingContext(sparkConf,Seconds(5))
    //读取配置文件
    val prop = new Properties();
    // 使用ClassLoader加载properties配置文件生成对应的输入流
    val in = sparkSteamReConsitution.getClass.getClassLoader().getResourceAsStream("application.properties");
    // 使用properties对象加载输入流
    prop.load(in)

    /**
     * 流计算设备激活信息
     */
    streamingContext.checkpoint("./saveCheckPoint1")
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> prop.getProperty("kafkaParams.bootstrap.servers"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "launch"
    )

    val topics = List("launch")
    val topicsPay = List("pay")
    /*val topicPartition = new TopicPartition("launch", 1)
    val offset:mutable.Map[TopicPartition, Long] = mutable.Map()
    offset += (topicPartition->0L)*/

    //付费topic
    val kafkaDStreamForPay = KafkaUtils.createDirectStream(streamingContext,LocationStrategies.PreferConsistent,Subscribe[String,String](topicsPay, kafkaParams))
    kafkaDStreamForPay.map(x=>{
      //println(x.value)
      val deviceMap  = JsonParser(x.value).convertTo[Map[String,String]]
      val statusInRedis = isNewDeviceInRedis(deviceMap,prop)
      var temp:(String,String,String) = null
      if(statusInRedis == null){
        //新设备
        temp = handleNewPayConsumerRecord(deviceMap,prop)
        //throw new Exception("pay通道较与launch通道先处理")
      } else {
        //旧设备
        temp = handleOldPayConsumerRecord(deviceMap,prop)
      }
      (temp,statusInRedis)
    }).reduceByKey(_+_).foreachRDD(rdd=>{
      rdd.foreachPartition(data=>{
        //println("pay")
        payData(data,prop)
      })
    })

    val kafkaDStream = KafkaUtils.createDirectStream(streamingContext,LocationStrategies.PreferConsistent,Subscribe[String,String](topics, kafkaParams))
    val wordStream = kafkaDStream.map(x=>{
      //println(x.topic)
      val deviceMap  = JsonParser(x.value).convertTo[Map[String,String]]
      val statusInRedis = isNewDeviceInRedis(deviceMap,prop)
      var temp:(String,String,String) = null
      if(statusInRedis == null){
        //新设备
        temp = handleNewLaunchConsumerRecord(deviceMap,prop)
      } else {
        //旧设备
        temp = handleOldLaunchConsumerRecord(deviceMap,prop)
      }
      //println(x.topic)
      (temp,statusInRedis)
    }).reduceByKey(_+_).foreachRDD(rdd=>{
      rdd.foreachPartition(data=>{
        launchData(data,prop)
      })
    }
    )

    streamingContext.start()
    streamingContext.awaitTermination()
  }

  /**
   * 查询redis是否新设备
   *
   * @param deviceMap 设备信息
   * @param prop 属性参数
   * @return deviceExistInRedis  新设备 null  旧设备 保存在redis中的字符串信息
   *
   */
  private def isNewDeviceInRedis(deviceMap:Map[String,String],prop:Properties) = {
    //根据 IMEI 或者 IDFA 设备码查找Redis缓存 判断是否为新设备 若存在缓存记录 为旧设备  若不存在 则为新设备并将设备码写入Redis
    try{
      redisUtil.connect(prop.getProperty("redis.server"))
    } catch {
      case ex:SocketTimeoutException=>{
        println("Redis连接超时")
      }
      case ex:Exception=>{
        println(ex.getMessage)
      }
    }
    //val start = new Date().getTime
    val deviceExistInRedis = redisUtil.get(deviceMap("appid") + '-' + deviceMap("imei")).getOrElse(null)
    if(deviceExistInRedis != null){
      //如果启动时间不是今天 则更新redis中的启动时间
      val redisData = deviceExistInRedis.split(",")
      if(redisData(1) != TODAY){
        redisUtil.set(deviceMap("appid") + '-' + deviceMap("imei"),redisData(0) + ',' + TODAY)
      }
    }
    deviceExistInRedis
  }

  /**
   * 处理launch通道新设备的逻辑
   */
  private def handleNewLaunchConsumerRecord(deviceMap:Map[String,String],prop:Properties) = {
    //查找条件优先级 imei->oaid->android_id->mac->ip
    val sqls = mutable.LinkedHashMap[String,String](
      "imei"->"SELECT  * FROM log_android_click_data WHERE imei_md5=?",
      "oaid"->"SELECT  * FROM log_android_click_data WHERE oaid=?",
      "androidid"->"SELECT  * FROM log_android_click_data WHERE androidid_md5=?",
      "mac"->"SELECT  * FROM log_android_click_data WHERE mac_md5=?",
      "ip"->"SELECT  * FROM log_android_click_data WHERE ip=?"
    )
    ////////////////新设备
    val advAscribeInfo:mutable.Map[String,String] = mutable.Map[String,String](deviceMap.toSeq:_*)    //immutable.map 转 mutable.map
    advAscribeInfo += ("plan_id"->"0","channel_id"->"0")

    val connection: Connection = DriverManager.getConnection(prop.getProperty("mysql.jdbc"), "default", "")
    //查找7天内的clickhouse数据进行归因
    val loop = new Breaks;
    loop.breakable {
      for ((k, sql) <- sqls) {
        val prep = connection.prepareStatement(sql)
        k match {
          case "imei"=>prep.setString(1, deviceMap("imei"))
          case "oaid"=>prep.setString(1, deviceMap("oaid"))
          case "androidid"=>prep.setString(1, deviceMap("androidid"))
          case "mac"=>prep.setString(1, deviceMap("mac"))
          case "ip"=>prep.setString(1, deviceMap("ip"))
        }
        val res = prep.executeQuery
        //广告归因信息
        while (res.next()) {
          advAscribeInfo("plan_id") = res.getString("plan_id")
          advAscribeInfo("channel_id") = res.getString("channel_id")
          //println(advAscribeInfo)
          loop.break()
        }

      }
    }
    //写入激活表
    val activeSql = "INSERT INTO test.datadvs_android_active(appid, imei_md5, oaid, androidid_md5, mac_md5, ip, plan_id, channel_id, active_time) VALUES(?,?,?,?,?,?,?,?,?)"
    val activePrep = connection.prepareStatement(activeSql)
    activePrep.setString(1,advAscribeInfo("appid"))
    activePrep.setString(2,advAscribeInfo("imei"))
    activePrep.setString(3,advAscribeInfo("oaid"))
    activePrep.setString(4,advAscribeInfo("androidid"))
    activePrep.setString(5,advAscribeInfo("mac"))
    activePrep.setString(6,advAscribeInfo("ip"))
    activePrep.setString(7,advAscribeInfo("plan_id"))
    activePrep.setString(8,advAscribeInfo("channel_id"))
    activePrep.setString(9,NOW)
    activePrep.executeQuery
    //写入启动表
    val launchLogSql = "INSERT INTO test.datadvs_android_launch_log(appid, imei_md5, oaid, androidid_md5, mac_md5, ip, plan_id, channel_id, launch_time) VALUES(?,?,?,?,?,?,?,?,?)"
    val launchLogPrep = connection.prepareStatement(launchLogSql)
    launchLogPrep.setString(1,advAscribeInfo("appid"))
    launchLogPrep.setString(2,advAscribeInfo("imei"))
    launchLogPrep.setString(3,advAscribeInfo("oaid"))
    launchLogPrep.setString(4,advAscribeInfo("androidid"))
    launchLogPrep.setString(5,advAscribeInfo("mac"))
    launchLogPrep.setString(6,advAscribeInfo("ip"))
    launchLogPrep.setString(7,advAscribeInfo("plan_id"))
    launchLogPrep.setString(8,advAscribeInfo("channel_id"))
    launchLogPrep.setString(9,NOW)
    launchLogPrep.executeQuery
    //写入redis  设备激活时间+启动更新时间
    redisUtil.set(advAscribeInfo("appid") + '-' + deviceMap("imei"),TODAY + ',' + TODAY)
    connection.close()
    (advAscribeInfo("plan_id"),advAscribeInfo("channel_id"),"new")
  }

  /**
   * 处理 launch 通道旧设备的逻辑
   */
  private def handleOldLaunchConsumerRecord(deviceMap:Map[String,String],prop:Properties) = {
    ////////////////////旧设备
    val advAscribeInfo:mutable.Map[String,String] = mutable.Map[String,String](deviceMap.toSeq:_*)
    advAscribeInfo += ("plan_id"->"0","channel_id"->"0")

    val connection: Connection = DriverManager.getConnection(prop.getProperty("clickhouse.jdbc"), "default", "")
    val sql = "SELECT * FROM test.datadvs_android_active WHERE appid=? AND imei_md5=?"
    val prep = connection.prepareStatement(sql)
    prep.setString(1,deviceMap("appid"))
    prep.setString(2,deviceMap("imei"))
    val res = prep.executeQuery
    while (res.next()) {
      advAscribeInfo("plan_id") = res.getString("plan_id")
      advAscribeInfo("channel_id") = res.getString("channel_id")
    }
    //写入启动表
    val launchLogSql = "INSERT INTO test.datadvs_android_launch_log(appid, imei_md5, oaid, androidid_md5, mac_md5, ip, plan_id, channel_id, launch_time) VALUES(?,?,?,?,?,?,?,?,?)"
    val launchLogPrep = connection.prepareStatement(launchLogSql)
    launchLogPrep.setString(1,advAscribeInfo("appid"))
    launchLogPrep.setString(2,advAscribeInfo("imei"))
    launchLogPrep.setString(3,advAscribeInfo("oaid"))
    launchLogPrep.setString(4,advAscribeInfo("androidid"))
    launchLogPrep.setString(5,advAscribeInfo("mac"))
    launchLogPrep.setString(6,advAscribeInfo("ip"))
    launchLogPrep.setString(7,advAscribeInfo("plan_id"))
    launchLogPrep.setString(8,advAscribeInfo("channel_id"))
    launchLogPrep.setString(9,NOW)
    launchLogPrep.executeQuery


    connection.close()
    (advAscribeInfo("plan_id"),advAscribeInfo("channel_id"),"old")
  }

  /**
   * 处理 pay 通道新设备的逻辑
   * 实际生产中 这段逻辑被调用的概率应该很低
   * 因为正常来说 launch 通道的数据肯定会较 pay 通道的数据先得到处理
   */
  private def handleNewPayConsumerRecord(deviceMap:Map[String,String],prop:Properties) = {
    //查找条件优先级 imei->oaid->android_id->mac->ip
    val sqls = mutable.LinkedHashMap[String,String](
      "imei"->"SELECT  * FROM test.datadvs_android_click_log WHERE imei_md5=?",
      "oaid"->"SELECT  * FROM test.datadvs_android_click_log WHERE oaid=?",
      "androidid"->"SELECT  * FROM test.datadvs_android_click_log WHERE androidid_md5=?",
      "mac"->"SELECT  * FROM test.datadvs_android_click_log WHERE mac_md5=?",
      "ip"->"SELECT  * FROM test.datadvs_android_click_log WHERE ip=?"
    )
    ////////////////新设备
    val advAscribeInfo:mutable.Map[String,String] = mutable.Map[String,String](deviceMap.toSeq:_*)    //immutable.map 转 mutable.map
    advAscribeInfo += ("plan_id"->"0","channel_id"->"0")

    val connection: Connection = DriverManager.getConnection(prop.getProperty("clickhouse.jdbc"), "default", "")
    //查找7天内的clickhouse数据进行归因
    val loop = new Breaks;
    loop.breakable {
      for ((k, sql) <- sqls) {
        val prep = connection.prepareStatement(sql)
        k match {
          case "imei"=>prep.setString(1, deviceMap("imei"))
          case "oaid"=>prep.setString(1, deviceMap("oaid"))
          case "androidid"=>prep.setString(1, deviceMap("androidid"))
          case "mac"=>prep.setString(1, deviceMap("mac"))
          case "ip"=>prep.setString(1, deviceMap("ip"))
        }
        val res = prep.executeQuery
        //广告归因信息
        while (res.next()) {
          advAscribeInfo("plan_id") = res.getString("plan_id")
          advAscribeInfo("channel_id") = res.getString("channel_id")
          //println(advAscribeInfo)
          loop.break()
        }

      }
    }
    //写入付费日志表
    val activeSql = "INSERT INTO test.datadvs_android_pay_log(appid, imei_md5, oaid, androidid_md5, mac_md5, ip, plan_id, channel_id, pay_time,pay_amount) VALUES(?,?,?,?,?,?,?,?,?,?)"
    val activePrep = connection.prepareStatement(activeSql)
    activePrep.setString(1,advAscribeInfo("appid"))
    activePrep.setString(2,advAscribeInfo("imei"))
    activePrep.setString(3,advAscribeInfo("oaid"))
    activePrep.setString(4,advAscribeInfo("androidid"))
    activePrep.setString(5,advAscribeInfo("mac"))
    activePrep.setString(6,advAscribeInfo("ip"))
    activePrep.setString(7,advAscribeInfo("plan_id"))
    activePrep.setString(8,advAscribeInfo("channel_id"))
    activePrep.setString(9,NOW)
    activePrep.setString(10,advAscribeInfo("amount"))
    activePrep.executeQuery

    connection.close()
    (advAscribeInfo("plan_id"),advAscribeInfo("channel_id"),advAscribeInfo("amount"))
  }

  /**
   * 处理 pay 通道旧设备的逻辑
   * TODO 计划信息应该直接在Redis中读取 不再从数据库中读取
   */
  private def handleOldPayConsumerRecord(deviceMap:Map[String,String],prop:Properties) = {
    ////////////////////旧设备
    val advAscribeInfo:mutable.Map[String,String] = mutable.Map[String,String](deviceMap.toSeq:_*)  //immutable 转 mutable
    advAscribeInfo += ("plan_id"->"0","channel_id"->"0")

    val connection: Connection = DriverManager.getConnection(prop.getProperty("clickhouse.jdbc"), "default", "")
    val sql = "SELECT * FROM test.datadvs_android_active WHERE appid=? AND imei_md5=?"
    val prep = connection.prepareStatement(sql)
    prep.setString(1,deviceMap("appid"))
    prep.setString(2,deviceMap("imei"))
    val res = prep.executeQuery
    while (res.next()) {
      advAscribeInfo("plan_id") = res.getString("plan_id")
      advAscribeInfo("channel_id") = res.getString("channel_id")
    }
    //写入付费日志表
    val launchLogSql = "INSERT INTO test.datadvs_android_pay_log(appid, imei_md5, oaid, androidid_md5, mac_md5, ip, plan_id, channel_id, pay_time,pay_amount) VALUES(?,?,?,?,?,?,?,?,?,?)"
    val launchLogPrep = connection.prepareStatement(launchLogSql)
    launchLogPrep.setString(1,advAscribeInfo("appid"))
    launchLogPrep.setString(2,advAscribeInfo("imei"))
    launchLogPrep.setString(3,advAscribeInfo("oaid"))
    launchLogPrep.setString(4,advAscribeInfo("androidid"))
    launchLogPrep.setString(5,advAscribeInfo("mac"))
    launchLogPrep.setString(6,advAscribeInfo("ip"))
    launchLogPrep.setString(7,advAscribeInfo("plan_id"))
    launchLogPrep.setString(8,advAscribeInfo("channel_id"))
    launchLogPrep.setString(9,NOW)
    launchLogPrep.setString(10,advAscribeInfo("amount"))
    launchLogPrep.executeQuery

    connection.close()
    (advAscribeInfo("plan_id"),advAscribeInfo("channel_id"),advAscribeInfo("amount"))
  }

  /**
   * launch通道 基础和留存数据的统计
   */
  private def launchData(data:Iterator[((String,String,String),String)],prop:Properties) = {
    Try {
      val connection = DriverManager.getConnection(prop.getProperty("mysql.jdbc"), "root", "")
      try {
        //println(data)
        //TODO 批量写入和更新基础统计数据
        val planExistSql = "SELECT * FROM test.statistics_data WHERE plan_id=? AND stat_date=?"
        val statPrep = connection.prepareStatement(planExistSql)

        val planExistSqlRet = "SELECT * FROM test.statistics_retention WHERE plan_id=? AND active_day=? AND retention_days=?"
        val statPrepRet = connection.prepareStatement(planExistSqlRet)

        for (row <- data) {
          print(row._1 + "|" + row._2)
          //计划基础数据更新和添加 start
          statPrep.setString(1, row._1._1)
          statPrep.setString(2, TODAY)
          val res = statPrep.executeQuery
          if (res.next()) {
            //如果该计划已有该天的统计记录  则进行数据更新
            var updatePrep: PreparedStatement = null
            if (row._1._3 == "old") {
              val updateSql = "UPDATE test.statistics_data SET launch_count=launch_count+? WHERE plan_id=?"
              updatePrep = connection.prepareStatement(updateSql)
              updatePrep.setInt(1, 1)
              updatePrep.setString(2, row._1._1)
            } else {
              val updateSql = "UPDATE test.statistics_data SET launch_count=launch_count+?,active_count=active_count+? WHERE plan_id=?"
              updatePrep = connection.prepareStatement(updateSql)
              updatePrep.setInt(1, 1)
              updatePrep.setInt(2, 1)
              updatePrep.setString(3, row._1._1)
            }
            updatePrep.executeUpdate
          } else {
            //如果该计划没有该天的统计数据  则写入一条统计记录
            if (row._1._3 == "old") {
              throw new Exception("计划数据写入异常")
            } else {
              val insertSql = "INSERT INTO test.statistics_data(plan_id,channel_id,launch_count,active_count,stat_date) VALUES(?,?,?,?,?)"
              //println(insertSql)
              val insertPrep = connection.prepareStatement(insertSql)
              insertPrep.setString(1, row._1._1)
              insertPrep.setString(2, row._1._2)
              insertPrep.setInt(3, 1)
              insertPrep.setInt(4, 1)
              insertPrep.setString(5, TODAY)
              insertPrep.executeUpdate
            }
          }
          //计划基础数据更新和添加 end

          //留存start
          //旧设备才会有留存数据
          if(row._1._3 == "old"){
            val arr = row._2.split(",")
            val active_day  = arr(0)
            val launch_day = arr(1)
            //激活日期和启动日期都不是当天的才会有留存数据
            if(active_day != TODAY && launch_day != TODAY){
              statPrepRet.setString(1, row._1._1)
              statPrepRet.setString(2, active_day)
              //计算留存天数
              val retention_days = diffDays(active_day,TODAY)
              statPrepRet.setInt(3, retention_days)
              val resRet = statPrepRet.executeQuery
              //查询留存记录表中是否存在记录 有记录更新 无记录写入
              if(resRet.next()){
                val updateSql = "UPDATE test.statistics_retention SET retention_count=retention_count+? WHERE plan_id=? AND active_day=? AND retention_days=?"
                val updatePrep = connection.prepareStatement(updateSql)
                updatePrep.setInt(1,1)
                updatePrep.setString(2,row._1._1)
                updatePrep.setString(3,active_day)
                updatePrep.setInt(4,retention_days)
                updatePrep.executeUpdate
              } else {
                val insertSql = "INSERT INTO test.statistics_retention(plan_id,channel_id,retention_count,retention_days,active_day) VALUES(?,?,?,?,?)"
                val insertPrep = connection.prepareStatement(insertSql)
                insertPrep.setString(1,row._1._1)
                insertPrep.setString(2,row._1._2)
                insertPrep.setInt(3,1)
                insertPrep.setInt(4,retention_days)
                insertPrep.setString(5,active_day)
                insertPrep.executeUpdate
              }
            }
          }
          //留存end

        }
        connection.close()
      } catch {
        case e: Exception => {
          println(e.getMessage)
        }
      } finally {
        closeMySQLConnection(connection)
      }
    }
  }

  /**
   * pay通道 付费和LTV数据的统计
   */
  private def payData(data:Iterator[((String,String,String),String)],prop:Properties) = {

      val connection = DriverManager.getConnection(prop.getProperty("mysql.jdbc"), "root", "")
      try {
        val planExistSql = "SELECT * FROM test.statistics_pay WHERE plan_id=? AND active_date=? AND pay_days=?"
        val statPrep = connection.prepareStatement(planExistSql)

        for (row <- data) {
          print(row._1 + "|" + row._2)
          //付费数据更新和添加 start
          var active_date = TODAY
          var pay_days = 1
          //row._2 为null 说明付费早于激活计算 但判定为当天的新设备
          if(row._2 != null){
            val arr = row._2.split(",")
            active_date  = arr(0)
            pay_days = diffDays(active_date,TODAY) + 1  //跟留存的天数稍有不同 需要+1
          }
          statPrep.setString(1, row._1._1)
          statPrep.setString(2, active_date)
          statPrep.setInt(3, pay_days)
          val res = statPrep.executeQuery
          if (res.next()) {
            //更新付费统计
            val updateSql = "UPDATE test.statistics_pay SET pay_amount=pay_amount+?,pay_count=pay_count+1 WHERE plan_id=? AND active_date=? AND pay_days=?"
            val updatePrep = connection.prepareStatement(updateSql)
            updatePrep.setInt(1, row._1._3.toInt)
            updatePrep.setString(2, row._1._1)
            updatePrep.setString(3, active_date)
            updatePrep.setInt(4, pay_days)
            updatePrep.executeUpdate()
          } else {
            //新增付费统计
            val insertSql = "INSERT INTO test.statistics_pay(plan_id,channel_id,pay_amount,pay_count,pay_days,active_date,pay_date) VALUES(?,?,?,?,?,?,?)"
            val insertPrep = connection.prepareStatement(insertSql)
            insertPrep.setString(1, row._1._1)
            insertPrep.setString(2, row._1._2)
            insertPrep.setInt(3, row._1._3.toInt)
            insertPrep.setInt(4, 1)
            insertPrep.setInt(5, pay_days)
            insertPrep.setString(6, active_date)
            insertPrep.setString(7, TODAY)
            insertPrep.executeUpdate()
          }
          //付费数据更新和添加 end
        }
        connection.close()
      } catch {
        case e: Exception => {
          println(e.getMessage)
        }
      } finally {
        closeMySQLConnection(connection)
      }

  }

  /**
   * spark更新函数
   * @param values
   * @param state
   * @return
   */
  def updateFunc(values:Seq[Int],state:Option[Int]):Option[Int] = {
    val _old = state.getOrElse(0)
    val _new = values.sum
    Some(_old + _new)
  }

  def reduceFunc(params1:mutable.Map[String,String],params2:mutable.Map[String,String]):mutable.Map[String,String] = {
    println("left---------------"+params1)
    println("right---------------"+params2)
    mutable.Map[String,String](("hi"->"spark"))
  }

  /**
   * MD5哈希函数
   * @param content
   * @return
   */
  private def hashMD5(content: String): String = {
    val md5 = MessageDigest.getInstance("MD5")
    val encoded = md5.digest((content).getBytes)
    encoded.map("%02x".format(_)).mkString
  }

  /**
   * 计算两个日期跨度的天数
   * startDate 起始日期
   * endDate  结束日期
   */
  private def diffDays(startDate:String,endDate:String):Int = {
    val dft = new SimpleDateFormat("yyyy-MM-dd")

    val start = dft.parse(startDate)
    val end = dft.parse(endDate)
    val starTime = start.getTime
    val endTime = end.getTime
    val num = ((endTime - starTime)/1000).toInt  //时间戳相差的毫秒数
    //System.out.println("相差天数为：" + num / 24 / 60 / 60 / 1000) //除以一天的毫秒数
    num / 24 / 60 / 60

  }

  /**
   * 关闭 MySQL 连接
   */
  private def closeMySQLConnection(con:Connection , sta:Statement = null, rs:ResultSet = null): Unit ={
    try {
      if (rs != null) rs.close
    } catch {
      case e: Exception => println(e.getMessage)
    }

    try {
      if (sta != null) sta.close
    } catch {
      case e: Exception => println(e.getMessage)
    }

    try {
      if (con != null) con.close
    } catch {
      case e: Exception => println(e.getMessage)
    }
  }
}


