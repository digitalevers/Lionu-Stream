/**
 * sparkStream重构版
 */


import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.kafka010.{KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.kafka.common.serialization.StringDeserializer

import java.sql.{Connection, PreparedStatement, ResultSet, Statement}
import java.net.SocketTimeoutException
import java.security.MessageDigest
import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import scala.collection.mutable
import util.control.Breaks._
import spray.json.{DefaultJsonProtocol, JsValue, JsonParser, RootJsonFormat}


//定义对应json的实体类
//case class redisDeviceInfo
//(
//  activetime:String,
//  launchtime:String,
//  planid:String,
//  channelid:String
//)

case class launchDeviceInfo(
                           androidid:String,
                           appName:String,
                           appid:String,
                           applicationId:String,
                           channel:String,
                           imei:String,
                           ip:String,
                           externalip:String,
                           mac:String,
                           model:String,
                           oaid:String,
                           os:Int,
                           planid:String,
                           sys:Int,
                           time:String,
                           ua:String,
                           versionCode:Int,
                           versionName:String)
case class regDeviceInfo(
                          androidid:String,
                          appName:String,
                          appid:String,
                          applicationId:String,
                          channel:String,
                          imei:String,
                          ip:String,
                          externalip:String,
                          mac:String,
                          model:String,
                          oaid:String,
                          os:Int,
                          planid:String,
                          sys:Int,
                          time:String,
                          ua:String,
                          versionCode:Int,
                          versionName:String)
case class payDeviceInfo(
                          amount:String,
                          androidid:String,
                          appName:String,
                          appid:String,
                          applicationId:String,
                          channel:String,
                          imei:String,
                          ip:String,
                          externalip:String,
                          mac:String,
                          model:String,
                          oaid:String,
                          os:Int,
                          planid:String,
                          sys:Int,
                          time:String,
                          ua:String,
                          versionCode:Int,
                          versionName:String)


//定义解析协议
object ResultJsonProtocol extends DefaultJsonProtocol {
  //implicit val redisDeviceInfoFormat = jsonFormat(redisDeviceInfo,"activetime","launchtime","planid","channedid")
  implicit val launchDeviceInfoFormat: RootJsonFormat[launchDeviceInfo] = jsonFormat(launchDeviceInfo, "androidid", "appName", "appid", "applicationId", "channel", "imei", "ip", "externalip", "mac", "model", "oaid", "os", "planid", "sys", "time", "ua", "versionCode", "versionName")
  implicit val regDeviceInfoFormat: RootJsonFormat[regDeviceInfo] = jsonFormat(regDeviceInfo, "androidid", "appName", "appid", "applicationId", "channel", "imei", "ip", "externalip", "mac", "model", "oaid", "os", "planid", "sys", "time", "ua", "versionCode", "versionName")
  implicit val payDeviceInfoFormat: RootJsonFormat[payDeviceInfo] = jsonFormat(payDeviceInfo, "amount", "androidid", "appName", "appid", "applicationId", "channel", "imei", "ip", "externalip", "mac", "model", "oaid", "os", "planid", "sys", "time", "ua", "versionCode", "versionName")
}

import ResultJsonProtocol._

object sparkSteamReConsitution {
  //private  val  NOW = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date())
  //private  val TODAY = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
  private def getNOW() = {
    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date())
  }

  private def getTODAY() = {
    new SimpleDateFormat("yyyy-MM-dd").format(new Date())
  }

  /**
   * 根据 os 的值获取不同的终端类型在 Redis 中的特征key
   * 即标识设备是否已存入 Redis 的属性值
   * @param os
   */
//  private def getRedisMetric(os:String) = {
//    os match {
//      case "1"=>"oaid"
//      case "2"=>"uuid"
//      case _=>"default"
//    }
//  }

  /**
   * 根据 os 的值获取不同的终端类型在 Redis 中的特征key
   * 即标识设备是否已存入 Redis 的属性值
   *
   * @param os
   */
  private val getRedisMetric = Map(
    "1"->"oaid",
    "2"->"uuid"
  )


  private def getKafkaParams(_prop:Properties,_topic:String) = {
    val _map =  Map[String, Object](
      "bootstrap.servers" -> _prop.getProperty("kafkaParams.bootstrap.servers"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> _topic
    )
     (List(_topic),_map)
  }

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
    /*val topicPartition = new TopicPartition("launch", 1)
    val offset:mutable.Map[TopicPartition, Long] = mutable.Map()
    offset += (topicPartition->0L)*/

    // 1.激活topic
    val launchKafkaParams = this.getKafkaParams(prop,"launch")
    //println(launchKafkaParams)
    val kafkaDStreamForLaunch = KafkaUtils.createDirectStream(streamingContext,LocationStrategies.PreferConsistent,Subscribe[String,String](launchKafkaParams._1, launchKafkaParams._2))
    kafkaDStreamForLaunch.map(x=>{
      println(x.value)
      //val deviceOriginMap  = getObjectProperties(JsonParser(x.value).convertTo[launchDeviceInfo])
      val deviceOriginMap  = JsonParser(x.value).convertTo[Map[String,String]]
      println(deviceOriginMap)

      var advAscribeInfo:Map[String,Any] = null                         //返回的归因信息
      var infoStorage = isNewDeviceInRedis(deviceOriginMap,prop)        //返回 Redis 中存放的json信息(激活时间，登录时间，计划id，渠道id)
      if( infoStorage == null ){
        infoStorage =  isNewDeviceInMySQL(deviceOriginMap)              //查找 MySQL 中返回json(激活时间，登录时间，计划id，渠道id)

        if(infoStorage == null){
          //新设备
          advAscribeInfo = handleNewLaunch(deviceOriginMap)
        } else {
          //旧设备
          //println(infoStorage)
          val infoStorageMap = JsonParser(infoStorage).convertTo[Map[String, String]]
          advAscribeInfo = handleOldLaunch(deviceOriginMap, infoStorageMap)
        }
      } else {
        //旧设备 传入redis的数据 写launch表不需要再查询
        //val infoObject = JsonParser(infoStorage).convertTo[redisDeviceInfo]
        //println(infoStorage)
        val infoStorageMap = JsonParser(infoStorage).convertTo[Map[String,String]]
        advAscribeInfo = handleOldLaunch(deviceOriginMap,infoStorageMap)
      }
      //println("launch-"+advAscribeInfo)
      advAscribeInfo
    }).foreachRDD(rdd=>{
        rdd.foreachPartition(iter=>{
          launchData(iter)
        })
      }
    )


    // 2. 注册topic
    val regKafkaParams = this.getKafkaParams(prop, "reg")
    //println(launchKafkaParams)
    val kafkaDStreamForReg = KafkaUtils.createDirectStream(streamingContext, LocationStrategies.PreferConsistent, Subscribe[String, String](regKafkaParams._1, regKafkaParams._2))
    kafkaDStreamForReg.map(x => {
      //println(x.topic)
      //val deviceOriginMap = getObjectProperties(JsonParser(x.value).convertTo[regDeviceInfo])
      val deviceOriginMap = JsonParser(x.value).convertTo[Map[String,String]]
      //println(deviceOriginMap)
      var advAscribeInfo: Map[String, Any] = null //返回的归因信息
      var infoStorage = isNewDeviceInRedis(deviceOriginMap, prop) //返回 Redis 中存放的json信息(激活时间，登录时间，计划id，渠道id)
      if (infoStorage == null) {
        infoStorage = isNewDeviceInMySQL(deviceOriginMap) //查找 MySQL 中返回json(激活时间，登录时间，计划id，渠道id)
        if (infoStorage == null) {
          //新设备
          throw new Exception("注册通道：没有找到存储的激活消息")
        } else {
          //旧设备
          //println(infoStorage)
          val infoStorageMap = JsonParser(infoStorage).convertTo[Map[String, String]]
          advAscribeInfo = handleOldReg(deviceOriginMap, infoStorageMap)
        }
      } else {
        //旧设备 传入redis的数据 写 reg 表不需要再查询
        //val infoObject = JsonParser(infoStorage).convertTo[redisDeviceInfo]
        //println(infoStorage)
        val infoStorageMap = JsonParser(infoStorage).convertTo[Map[String, String]]
        advAscribeInfo = handleOldReg(deviceOriginMap, infoStorageMap)
      }
      //println("reg-"+advAscribeInfo)
      advAscribeInfo
    }).foreachRDD(rdd => {
        rdd.foreachPartition(iter => {
          regData(iter)
        })
      }
    )


    // 3.付费topic
    val payKafkaParams = this.getKafkaParams(prop, "pay")
    //println(payKafkaParams)
    val kafkaDStreamForPay = KafkaUtils.createDirectStream(streamingContext, LocationStrategies.PreferConsistent, Subscribe[String, String](payKafkaParams._1, payKafkaParams._2))
    kafkaDStreamForPay.map(x => {
      //println(x.value)
      //val deviceOriginMap = getObjectProperties(JsonParser(x.value).convertTo[payDeviceInfo])
      val deviceOriginMap = JsonParser(x.value).convertTo[Map[String,String]]
      var advAscribeInfo:Map[String,Any] = null
      var infoStorage = isNewDeviceInRedis(deviceOriginMap, prop)

      if (infoStorage == null) {
        infoStorage =  isNewDeviceInMySQL(deviceOriginMap)
        if(infoStorage == null){
          //新设备
          //advAscribeInfo = handleNewPayConsumerRecord(deviceOriginMap)
          throw new Exception("付费通道：没有找到存储的激活消息")
        } else {
          //旧设备
          val infoStorageMap = JsonParser(infoStorage).convertTo[Map[String, String]]
          advAscribeInfo = handleOldPay(deviceOriginMap, infoStorageMap)
        }
        //throw new Exception("pay通道比launch通道先处理")
      } else {
        //旧设备
        val infoStorageMap = JsonParser(infoStorage).convertTo[Map[String,String]]
        advAscribeInfo = handleOldPay(deviceOriginMap,infoStorageMap)
      }
      //println("pay-"+advAscribeInfo)
      advAscribeInfo
    }).foreachRDD(rdd => {
      rdd.foreachPartition(data => {
        //println("pay")
        payData(data)
      })
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }

  /**
   * 查询redis是否新设备
   *
   * @param deviceMap 设备信息
   * @param prop 属性参数
   * @return deviceExistInRedis
   *         新设备返回null
   *         旧设备返回保存在redis中的激活信息
   *
   */
  private def isNewDeviceInRedis(deviceMap:Map[String,Any],prop:Properties) = {
    //根据 OAID 或者 IDFA 设备码查找Redis缓存 判断是否为新设备 若存在缓存记录 为旧设备  若不存在 则为新设备并将设备码写入Redis
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
    val metric = this.getRedisMetric(deviceMap("os").toString)
    val deviceExistInRedis = redisUtil.get(deviceMap("appid") + "-" + deviceMap(metric)).getOrElse(null)
    deviceExistInRedis
  }

  /**
   * 查询 mysql 激活表是否新设备
   * redis未查询到则再查询一次MySQL
   *
   * @param deviceMap 设备信息
   * @param prop      属性参数
   * @return deviceExistInMySQL
   *         新设备返回null
   *         旧设备返回保存在 MySQL 中的激活信息 格式为 "激活时间,最近启动时间"
   *         TODO 根据设备类型是Android还是iOS查找对应的数据库表
   */
  private def isNewDeviceInMySQL(deviceMap: Map[String, Any]): String = {
    deviceMap("os").toString match {
      case "1"=>this.isNewAndroidDeviceInMySQL(deviceMap)
      case "2"=>this.isNewiOSDeviceInMySQL(deviceMap)
      case _=>throw new Exception("platform error")
    }
  }

  private def isNewAndroidDeviceInMySQL(deviceMap: Map[String, Any]): String = {
    val metric = getRedisMetric(deviceMap("os").toString)
    val connection: Connection = JDBCutil.getConnection
    val activeExistSql = "SELECT active_time,plan_id,channel_id FROM log_android_active WHERE oaid_md5=? ORDER BY active_time LIMIT 0,1"
    val activeRes = JDBCutil.executeQuery(connection, activeExistSql, Array(deviceMap(metric)))
    if (activeRes.length == 0) {
      //新设备
      null
    } else {
      val launchExistSql = "SELECT launch_time FROM log_android_launch WHERE oaid_md5=? ORDER BY launch_time DESC LIMIT 0,1"
      val launchRes = JDBCutil.executeQuery(connection, launchExistSql, Array(deviceMap(metric)))
      if (launchRes.length == 0) {
        throw new Exception("有激活信息但是未查到启动信息")
      } else {
        //redisDeviceInfo(activeRes(0)(0),launchRes(0)(0),activeRes(0)(1),activeRes(0)(2)).toJson.compactPrint
        s"""{"activetime":"${activeRes(0)(0)}","launchtime":"${launchRes(0)(0)}","planid":"${activeRes(0)(1)}","channelid":"${activeRes(0)(2)}"}"""
      }
    }
  }

  /**
   * ["uuid"],["idfa"!=00000000-0000-0000-0000-000000000000],["internal ip","deviceModel","outernal ip"]
   *
   * @param deviceMap
   */
  private def isNewiOSDeviceInMySQL(deviceMap: Map[String, Any]): String = {
    val metric = getRedisMetric(deviceMap("os").toString)
    val connection: Connection = JDBCutil.getConnection

    val activeExistSql = "SELECT active_time,plan_id,channel_id FROM log_ios_active WHERE uuid_md5=? ORDER BY active_time LIMIT 0,1"
    val activeRes = JDBCutil.executeQuery(connection, activeExistSql, Array(deviceMap(metric)))
    if (activeRes.length == 0) {
      //新设备
      null
    } else {
      val launchExistSql = "SELECT launch_time FROM log_ios_launch WHERE uuid_md5=? ORDER BY launch_time DESC LIMIT 0,1"
      val launchRes = JDBCutil.executeQuery(connection, launchExistSql, Array(deviceMap(metric)))
      if (launchRes.length == 0) {
        throw new Exception("有激活信息但是未查到启动信息")
      } else {
        //redisDeviceInfo(activeRes(0)(0),launchRes(0)(0),activeRes(0)(1),activeRes(0)(2)).toJson.compactPrint
        s"""{"activetime":"${activeRes(0)(0)}","launchtime":"${launchRes(0)(0)}","planid":"${activeRes(0)(1)}","channelid":"${activeRes(0)(2)}"}"""
      }
    }
  }



  /**
   * 处理launch通道 新设备的逻辑
   */
  private def handleNewLaunch(deviceMap:Map[String,String]) = {
    deviceMap("os").toInt match {
      case 1 => this.handleNewLaunchAndroid(deviceMap)
      case 2 => this.handleNewLaunchiOS(deviceMap)
    }
  }

  /**
   * 处理新的Android设备激活
   */
  private def handleNewLaunchAndroid(deviceMap:Map[String,String]) = {
    val NOW = this.getNOW()
    //查找条件优先级 imei->oaid->android_id->mac->ip
    val sqls = mutable.LinkedHashMap[String, String](
      "imei" -> "SELECT  * FROM log_android_click_data WHERE imei_md5=?",
      "oaid" -> "SELECT  * FROM log_android_click_data WHERE oaid_md5=?",
      "androidid" -> "SELECT  * FROM log_android_click_data WHERE androidid_md5=?",
      "mac" -> "SELECT  * FROM log_android_click_data WHERE mac_md5=?",
      "externalip" -> "SELECT  * FROM log_android_click_data WHERE external_ip=?"
    )
    ////////////////新设备
    val advAscribeInfo: mutable.Map[String, String] = mutable.Map[String, String](deviceMap.toSeq: _*) //immutable.map 转 mutable.map
    advAscribeInfo += ("plan_id" -> "0", "channel_id" -> "0")

    //val connection: Connection = DriverManager.getConnection(prop.getProperty("mysql.url"), prop.getProperty("mysql.user"), prop.getProperty("mysql.password"))
    val connection: Connection = JDBCutil.getConnection
    //查找7天内的 mysql 数据进行归因
    breakable {
      for ((k, sql) <- sqls) {
        val prep = connection.prepareStatement(sql)
        k match {
          case "imei" => prep.setString(1, deviceMap("imei"))
          case "oaid" => prep.setString(1, deviceMap("oaid"))
          case "androidid" => prep.setString(1, deviceMap("androidid"))
          case "mac" => prep.setString(1, deviceMap("mac"))
          case "externalip" => prep.setString(1, deviceMap("externalip"))
        }
        val res = prep.executeQuery
        //广告归因信息
        while (res.next()) {
          advAscribeInfo("plan_id") = res.getString("plan_id")
          advAscribeInfo("channel_id") = res.getString("channel_id")
          //println(advAscribeInfo)
          break()
        }
      }
    }
    //写入激活表
    val insertActiveSql = "INSERT INTO log_android_active(appid, imei_md5, oaid_md5, androidid_md5, mac_md5, ip, external_ip, plan_id, channel_id, active_time) VALUES(?,?,?,?,?,?,?,?,?,?)"
    JDBCutil.executeUpdate(connection, insertActiveSql, Array(advAscribeInfo("appid"), advAscribeInfo("imei"), advAscribeInfo("oaid"), advAscribeInfo("androidid"), advAscribeInfo("mac"), advAscribeInfo("ip"), advAscribeInfo("externalip"), advAscribeInfo("plan_id"), advAscribeInfo("channel_id"), NOW))

    //写入启动表
    val launchLogSql = "INSERT INTO log_android_launch(appid, imei_md5, oaid_md5, androidid_md5, mac_md5, ip, external_ip, plan_id, channel_id, launch_time) VALUES(?,?,?,?,?,?,?,?,?,?)"
    JDBCutil.executeUpdate(connection, launchLogSql, Array(advAscribeInfo("appid"), advAscribeInfo("imei"), advAscribeInfo("oaid"), advAscribeInfo("androidid"), advAscribeInfo("mac"), advAscribeInfo("ip"), advAscribeInfo("externalip"), advAscribeInfo("plan_id"), advAscribeInfo("channel_id"), NOW))
    connection.close()

    //写入Redis  key:appid-oaid  value:部分设备信息的json字符串
    //val redisInfo = redisDeviceInfo(NOW,NOW,advAscribeInfo("plan_id"),advAscribeInfo("channel_id"))
    //val partialDeviceInfoJson = redisInfo.toJson.compactPrint
    val partialDeviceInfoJson =
    s"""{"activetime":"${NOW}","launchtime":"${NOW}","planid":"${advAscribeInfo("plan_id")}","channelid":"${advAscribeInfo("channel_id")}"}"""

    redisUtil.set(advAscribeInfo("appid") + '-' + deviceMap("oaid"), partialDeviceInfoJson)
    Map(
      "appid" -> advAscribeInfo("appid"),
      "activetime" -> NOW,
      "launchtime" -> NOW,
      "planid" -> advAscribeInfo("plan_id"),
      "channelid" -> advAscribeInfo("channel_id"),
      "new" -> 1
    )
  }

  /**
   * 处理新的iOS设备激活
   * @param deviceMap
   */
  private def handleNewLaunchiOS(deviceMap:Map[String,String]) = {
    val NOW = this.getNOW()
    //查找条件优先级 ip->idfa->caid1->caid2
    val sqls = Map(
      "external_ip" -> "SELECT  * FROM log_ios_click_data WHERE external_ip=?",
      "idfa" -> "SELECT  * FROM log_ios_click_data WHERE idfa_md5=?"
      //"caid1"-> "SELECT  * FROM log_ios_click_data WHERE CAID1=?",
      //"caid2"-> "SELECT  * FROM log_ios_click_data WHERE CAID2=?"
    )
    ////////////////新设备
    val advAscribeInfo: mutable.Map[String, String] = mutable.Map[String, String](deviceMap.toSeq: _*) //immutable.map 转 mutable.map
    advAscribeInfo += ("plan_id" -> "0", "channel_id" -> "0")

    //val connection: Connection = DriverManager.getConnection(prop.getProperty("mysql.url"), prop.getProperty("mysql.user"), prop.getProperty("mysql.password"))
    val connection: Connection = JDBCutil.getConnection
    //查找7天内的 mysql 数据进行归因
    breakable {
      for ((k, sql) <- sqls) {
        val prep = connection.prepareStatement(sql)
        k match {
          case "external_ip" => prep.setString(1, deviceMap("externalip"))
          case "idfa" => prep.setString(1, deviceMap("idfa"))
        }
        val res = prep.executeQuery
        //广告归因信息
        while (res.next()) {
          advAscribeInfo("plan_id") = res.getString("plan_id")
          advAscribeInfo("channel_id") = res.getString("channel_id")
          //println(advAscribeInfo)
          break()
        }
      }
    }
    //写入激活表
    val insertActiveSql = "INSERT INTO log_ios_active(appid, uuid_md5, idfa_md5, model, ip, external_ip, plan_id, channel_id, active_time) VALUES(?,?,?,?,?,?,?,?,?)"
    JDBCutil.executeUpdate(connection, insertActiveSql, Array(advAscribeInfo("appid"), advAscribeInfo("uuid"), advAscribeInfo("idfa"), advAscribeInfo("deviceModel"), advAscribeInfo("ipAddress"), advAscribeInfo("externalip"), advAscribeInfo("plan_id"), advAscribeInfo("channel_id"), NOW))

    //写入启动表
    val launchLogSql = "INSERT INTO log_ios_launch(appid, uuid_md5, idfa_md5, model, ip, external_ip, plan_id, channel_id, launch_time) VALUES(?,?,?,?,?,?,?,?,?)"
    JDBCutil.executeUpdate(connection, launchLogSql, Array(advAscribeInfo("appid"), advAscribeInfo("uuid"), advAscribeInfo("idfa"), advAscribeInfo("deviceModel"), advAscribeInfo("ipAddress"), advAscribeInfo("externalip"),advAscribeInfo("plan_id"), advAscribeInfo("channel_id"), NOW))
    connection.close()

    //写入Redis  key:appid-oaid  value:部分设备信息的json字符串
    //val redisInfo = redisDeviceInfo(NOW,NOW,advAscribeInfo("plan_id"),advAscribeInfo("channel_id"))
    //val partialDeviceInfoJson = redisInfo.toJson.compactPrint
    val partialDeviceInfoJson =
    s"""{"activetime":"${NOW}","launchtime":"${NOW}","planid":"${advAscribeInfo("plan_id")}","channelid":"${advAscribeInfo("channel_id")}"}"""

    redisUtil.set(advAscribeInfo("appid") + '-' + deviceMap("uuid"), partialDeviceInfoJson)
    Map(
      "appid" -> advAscribeInfo("appid"),
      "activetime" -> NOW,
      "launchtime" -> NOW,
      "planid" -> advAscribeInfo("plan_id"),
      "channelid" -> advAscribeInfo("channel_id"),
      "new" -> 1
    )
  }


  /**
   * 处理 launch 通道旧设备的逻辑
   */
  private def handleOldLaunch(deviceMap:Map[String,String],infoStorageMap:Map[String,String]) = {
    deviceMap("os").toInt match {
      case 1 => this.handleOldLaunchAndroid(deviceMap,infoStorageMap)
      case 2 => this.handleOldLaunchiOS(deviceMap,infoStorageMap)
    }
  }


  private def handleOldLaunchAndroid(deviceMap:Map[String,String],infoStorageMap:Map[String,String]) = {
    val NOW = this.getNOW()
    ////////////////////旧设备
    val advAscribeInfo: mutable.Map[String, String] = mutable.Map[String, String](deviceMap.toSeq: _*)
    advAscribeInfo += ("plan_id" -> infoStorageMap("planid"), "channel_id" -> infoStorageMap("channelid"))
    //val connection: Connection = DriverManager.getConnection(prop.getProperty("mysql.url"), prop.getProperty("mysql.user"), prop.getProperty("mysql.password"))
    val connection: Connection = JDBCutil.getConnection
    //写入启动表
    val launchLogSql = "INSERT INTO log_android_launch(appid, imei_md5, oaid_md5, androidid_md5, mac_md5, ip, external_ip, plan_id, channel_id, launch_time) VALUES(?,?,?,?,?,?,?,?,?,?)"
    JDBCutil.executeUpdate(connection, launchLogSql, Array(advAscribeInfo("appid"), advAscribeInfo("imei"), advAscribeInfo("oaid"), advAscribeInfo("androidid"), advAscribeInfo("mac"), advAscribeInfo("ip"), advAscribeInfo("externalip"), advAscribeInfo("plan_id"), advAscribeInfo("channel_id"), NOW))
    connection.close()
    //写入redis  key:appid-oaid  value:json
    val partialDeviceInfoJson =
      s"""{"activetime":"${infoStorageMap("activetime")}","launchtime":"${NOW}","planid":"${advAscribeInfo("plan_id")}","channelid":"${advAscribeInfo("channel_id")}"}"""
    //println(partialDeviceInfoJson)
    var redisRes = redisUtil.set(advAscribeInfo("appid") + '-' + deviceMap("oaid"), partialDeviceInfoJson)
    if (redisRes == true) {
      Map(
        "appid" -> advAscribeInfo("appid"),
        "activetime" -> infoStorageMap("activetime"), //设备首次激活时间
        "launchtime" -> infoStorageMap("launchtime"), //最近上一次的设备启动时间
        "planid" -> advAscribeInfo("plan_id"),
        "channelid" -> advAscribeInfo("channel_id"),
        "new" -> 0
      )
    } else {
      throw new Exception("写入redis失败")
    }
  }


  private def handleOldLaunchiOS(deviceMap: Map[String, String],infoStorageMap:Map[String,String]) = {
    val NOW = this.getNOW()
    ////////////////////旧设备
    val advAscribeInfo =  deviceMap + ("plan_id" -> infoStorageMap("planid"), "channel_id" -> infoStorageMap("channelid"))
    //advAscribeInfo += ("plan_id" -> infoStorageMap("planid"), "channel_id" -> infoStorageMap("channelid"))
    //val connection: Connection = DriverManager.getConnection(prop.getProperty("mysql.url"), prop.getProperty("mysql.user"), prop.getProperty("mysql.password"))
    val connection: Connection = JDBCutil.getConnection
    //写入启动表
    val launchLogSql = "INSERT INTO log_ios_launch(appid, uuid_md5, idfa_md5, model, ip, external_ip, plan_id, channel_id, launch_time) VALUES(?,?,?,?,?,?,?,?,?)"
    JDBCutil.executeUpdate(connection, launchLogSql, Array(advAscribeInfo("appid"), advAscribeInfo("uuid"), advAscribeInfo("idfa"), advAscribeInfo("deviceModel"), advAscribeInfo("ipAddress"), advAscribeInfo("externalip"), advAscribeInfo("plan_id"), advAscribeInfo("channel_id"), NOW))
    connection.close()
    //写入redis  key:appid-oaid  value:json
    val partialDeviceInfoJson =
      s"""{"activetime":"${infoStorageMap("activetime")}","launchtime":"${NOW}","planid":"${advAscribeInfo("plan_id")}","channelid":"${advAscribeInfo("channel_id")}"}"""
    //println(partialDeviceInfoJson)
    var redisRes = redisUtil.set(advAscribeInfo("appid") + '-' + deviceMap("uuid"), partialDeviceInfoJson)
    if (redisRes == true) {
      Map(
        "appid" -> advAscribeInfo("appid"),
        "activetime" -> infoStorageMap("activetime"), //设备首次激活时间
        "launchtime" -> infoStorageMap("launchtime"), //最近上一次的设备启动时间
        "planid" -> advAscribeInfo("plan_id"),
        "channelid" -> advAscribeInfo("channel_id"),
        "new" -> 0
      )
    } else {
      throw new Exception("写入redis失败")
    }
  }


  /**
   * 处理 reg 通道旧设备的逻辑
   */
  private def handleOldReg(deviceMap: Map[String, String], infoStorageMap: Map[String, String]) = {
    deviceMap("os").toInt match {
      case 1 => this.handleOldRegAndroid(deviceMap, infoStorageMap)
      case 2 => this.handleOldRegiOS(deviceMap, infoStorageMap)
    }
  }

  private def handleOldRegAndroid(deviceMap: Map[String, String], infoStorageMap: Map[String, String]) = {
    val NOW = this.getNOW()
    ////////////////////旧设备
    val advAscribeInfo: mutable.Map[String, String] = mutable.Map[String, String](deviceMap.toSeq: _*)
    advAscribeInfo += ("plan_id" -> infoStorageMap("planid"), "channel_id" -> infoStorageMap("channelid"))
    //val connection: Connection = DriverManager.getConnection(prop.getProperty("mysql.url"), prop.getProperty("mysql.user"), prop.getProperty("mysql.password"))
    val connection: Connection = JDBCutil.getConnection
    //写入注册设备表
    val launchLogSql = "INSERT INTO log_android_reg(appid, imei_md5, oaid_md5, androidid_md5, mac_md5, ip, external_ip, plan_id, channel_id, reg_time) VALUES(?,?,?,?,?,?,?,?,?,?)"
    JDBCutil.executeUpdate(connection, launchLogSql, Array(advAscribeInfo("appid"), advAscribeInfo("imei"), advAscribeInfo("oaid"), advAscribeInfo("androidid"), advAscribeInfo("mac"), advAscribeInfo("ip"), advAscribeInfo("externalip"), advAscribeInfo("plan_id"), advAscribeInfo("channel_id"), NOW))
    connection.close()
    Map(
      "appid" -> advAscribeInfo("appid"),
      "activetime" -> infoStorageMap("activetime"),
      "launchtime" -> infoStorageMap("launchtime"),
      "planid" -> advAscribeInfo("plan_id"),
      "channelid" -> advAscribeInfo("channel_id"),
      "new" -> 0
    )
  }

  private def handleOldRegiOS(deviceMap: Map[String, String], infoStorageMap: Map[String, String]) = {
    val NOW = this.getNOW()
    ////////////////////旧设备
    val advAscribeInfo: mutable.Map[String, String] = mutable.Map[String, String](deviceMap.toSeq: _*)
    advAscribeInfo += ("plan_id" -> infoStorageMap("planid"), "channel_id" -> infoStorageMap("channelid"))
    //val connection: Connection = DriverManager.getConnection(prop.getProperty("mysql.url"), prop.getProperty("mysql.user"), prop.getProperty("mysql.password"))
    val connection: Connection = JDBCutil.getConnection
    //写入注册设备表
    val launchLogSql = "INSERT INTO log_ios_reg(appid, uuid_md5, idfa_md5, model, ip, external_ip, plan_id, channel_id, reg_time) VALUES(?,?,?,?,?,?,?,?,?)"
    JDBCutil.executeUpdate(connection, launchLogSql, Array(advAscribeInfo("appid"), advAscribeInfo("uuid"), advAscribeInfo("idfa"), advAscribeInfo("deviceModel"), advAscribeInfo("ipAddress"), advAscribeInfo("externalip"), advAscribeInfo("plan_id"), advAscribeInfo("channel_id"), NOW))
    connection.close()
    Map(
      "appid" -> advAscribeInfo("appid"),
      "activetime" -> infoStorageMap("activetime"),
      "launchtime" -> infoStorageMap("launchtime"),
      "planid" -> advAscribeInfo("plan_id"),
      "channelid" -> advAscribeInfo("channel_id"),
      "new" -> 0
    )
  }




  /**
   * 处理 pay 通道新设备的逻辑
   * 实际生产中 这段逻辑被调用的概率应该很低
   * 因为正常来说 launch 通道的数据肯定会较 pay 通道的数据先得到处理
   */
  /*private def handleNewPayConsumerRecord(deviceMap:Map[String,String]) = {
    val NOW = this.getNOW()
    //查找条件优先级 imei->oaid->android_id->mac->ip
    val sqls = mutable.LinkedHashMap[String,String](
      "imei"     ->"SELECT  * FROM log_android_click_data WHERE imei_md5=?",
              "oaid"     ->"SELECT  * FROM log_android_click_data WHERE oaid=?",
              "androidid"->"SELECT  * FROM log_android_click_data WHERE androidid_md5=?",
              "mac"      ->"SELECT  * FROM log_android_click_data WHERE mac_md5=?",
              "ip"       ->"SELECT  * FROM log_android_click_data WHERE ip=?"
    )
    ////////////////新设备
    val advAscribeInfo:mutable.Map[String,String] = mutable.Map[String,String](deviceMap.toSeq:_*)    //immutable.map 转 mutable.map
    advAscribeInfo += ("plan_id"->"0","channel_id"->"0")

    //val connection: Connection = DriverManager.getConnection(prop.getProperty("mysql.url"), prop.getProperty("mysql.user"), prop.getProperty("mysql.password"))
    val connection: Connection = JDBCutil.getConnection
    //查找7天内的 mysql 数据进行归因
    breakable {
      for ((k, sql) <- sqls) {
        val prep = connection.prepareStatement(sql)
        k match {
          case "imei"     =>prep.setString(1, deviceMap("imei"))
          case "oaid"     =>prep.setString(1, deviceMap("oaid"))
          case "androidid"=>prep.setString(1, deviceMap("androidid"))
          case "mac"      =>prep.setString(1, deviceMap("mac"))
          case "ip"       =>prep.setString(1, deviceMap("ip"))
        }
        val res = prep.executeQuery
        //广告归因信息
        while (res.next()) {
          advAscribeInfo("plan_id") = res.getString("plan_id")
          advAscribeInfo("channel_id") = res.getString("channel_id")
          //println(advAscribeInfo)
          break()
        }
      }
    }
    //写入付费日志表
    val payLogSql = "INSERT INTO log_android_pay(appid, imei_md5, oaid, androidid_md5, mac_md5, ip, plan_id, channel_id, pay_time,pay_amount) VALUES(?,?,?,?,?,?,?,?,?,?)"
    JDBCutil.executeUpdate(connection, payLogSql, Array(advAscribeInfo("appid"), advAscribeInfo("imei"), advAscribeInfo("oaid"), advAscribeInfo("androidid"), advAscribeInfo("mac"), advAscribeInfo("ip"), advAscribeInfo("plan_id"), advAscribeInfo("channel_id"), NOW, advAscribeInfo("amount")))
    connection.close()
    Map(
      "appid" -> advAscribeInfo("appid"),
      "activetime" -> NOW,
      "launchtime" -> NOW,
      "planid" -> advAscribeInfo("plan_id"),
      "channelid" -> advAscribeInfo("channel_id"),
      "new" -> 1,
      "amount"->advAscribeInfo("amount")
    )
  }*/

  /**
   * 处理 pay 通道旧设备的逻辑
   * TODO 计划信息应该直接在Redis中读取 不再从数据库中读取
   */
  private def handleOldPay(deviceMap:Map[String,String],infoStorageMap:Map[String,String]) = {
    deviceMap("os").toInt match {
      case 1 => this.handleOldPayAndroid(deviceMap, infoStorageMap)
      case 2 => this.handleOldPayiOS(deviceMap, infoStorageMap)
    }
  }


  private def handleOldPayAndroid(deviceMap: Map[String, String], infoStorageMap: Map[String, String]) = {
    val NOW = this.getNOW()
    ////////////////////旧设备
    val advAscribeInfo: mutable.Map[String, String] = mutable.Map[String, String](deviceMap.toSeq: _*) //immutable 转 mutable
    advAscribeInfo += ("plan_id" -> infoStorageMap("planid"), "channel_id" -> infoStorageMap("channelid"))
    val connection: Connection = JDBCutil.getConnection

    //写入付费日志表
    val payLogSql = "INSERT INTO log_android_pay(appid, imei_md5, oaid_md5, androidid_md5, mac_md5, ip, external_ip, plan_id, channel_id, pay_time,pay_amount) VALUES(?,?,?,?,?,?,?,?,?,?,?)"
    JDBCutil.executeUpdate(connection, payLogSql, Array(advAscribeInfo("appid"), advAscribeInfo("imei"), advAscribeInfo("oaid"), advAscribeInfo("androidid"), advAscribeInfo("mac"), advAscribeInfo("ip"), advAscribeInfo("externalip"), advAscribeInfo("plan_id"), advAscribeInfo("channel_id"), NOW, advAscribeInfo("amount")))
    connection.close()
    Map(
      "appid" -> advAscribeInfo("appid"),
      "activetime" -> infoStorageMap("activetime"),
      "launchtime" -> infoStorageMap("launchtime"),
      "planid" -> advAscribeInfo("plan_id"),
      "channelid" -> advAscribeInfo("channel_id"),
      "new" -> 0,
      "amount" -> advAscribeInfo("amount")
    )
  }


  private def handleOldPayiOS(deviceMap: Map[String, String], infoStorageMap: Map[String, String]) = {
    val NOW = this.getNOW()
    ////////////////////旧设备
    val advAscribeInfo: mutable.Map[String, String] = mutable.Map[String, String](deviceMap.toSeq: _*) //immutable 转 mutable
    advAscribeInfo += ("plan_id" -> infoStorageMap("planid"), "channel_id" -> infoStorageMap("channelid"))
    val connection: Connection = JDBCutil.getConnection

    //写入付费日志表
    val payLogSql = "INSERT INTO log_ios_pay(appid, uuid_md5, idfa_md5, model, ip, external_ip, plan_id, channel_id, pay_time, pay_amount) VALUES(?,?,?,?,?,?,?,?,?,?)"
    JDBCutil.executeUpdate(connection, payLogSql, Array(advAscribeInfo("appid"), advAscribeInfo("uuid"), advAscribeInfo("idfa"), advAscribeInfo("deviceModel"), advAscribeInfo("ipAddress"), advAscribeInfo("externalip"), advAscribeInfo("plan_id"), advAscribeInfo("channel_id"), NOW, advAscribeInfo("amount")))
    connection.close()
    Map(
      "appid" -> advAscribeInfo("appid"),
      "activetime" -> infoStorageMap("activetime"),
      "launchtime" -> infoStorageMap("launchtime"),
      "planid" -> advAscribeInfo("plan_id"),
      "channelid" -> advAscribeInfo("channel_id"),
      "new" -> 0,
      "amount" -> advAscribeInfo("amount")
    )
  }



  /**
   * launch通道 基础和留存数据的统计
   */
  private def launchData(data:Iterator[Map[String,Any]]) = {
    val TODAY = this.getTODAY()
    try {
      //val connection = DriverManager.getConnection(prop.getProperty("mysql.url"), prop.getProperty("mysql.user"), prop.getProperty("mysql.password"))
      val connection: Connection = JDBCutil.getConnection
      try {
        //println(data)
        //TODO 批量写入和更新基础统计数据
        val planExistSql = "SELECT * FROM statistics_base WHERE app_id=? AND plan_id=? AND stat_date=?"
        val statPrep = connection.prepareStatement(planExistSql)

        for (row <- data) {
          //start计划基础数据更新和添加
          statPrep.setString(1, row("appid").toString)
          statPrep.setString(2, row("planid").toString)
          statPrep.setString(3, TODAY)
          val res = statPrep.executeQuery
          if (res.next()) {
            //如果该计划已有该天的统计记录  则进行数据更新
            var updatePrep: PreparedStatement = null
            if (row("new") == 0) {
              val updateSql = "UPDATE statistics_base SET launch_count=launch_count+? WHERE app_id=? AND plan_id=? AND stat_date=?"
              JDBCutil.executeUpdate(connection,updateSql,Array(1, row("appid"),row("planid"),TODAY))
            } else {
              val updateSql = "UPDATE statistics_base SET launch_count=launch_count+?,active_count=active_count+? WHERE app_id=? AND plan_id=? AND stat_date=?"
              JDBCutil.executeUpdate(connection,updateSql,Array(1, 1, row("appid"),row("planid"),TODAY))
            }
          } else {
            //如果该计划没有该天的统计数据  则写入一条统计记录
            if(row("new") == 0){
              val insertSql = "INSERT INTO statistics_base(app_id,plan_id,channel_id,launch_count,stat_date) VALUES(?,?,?,?,?)"
              JDBCutil.executeUpdate(connection, insertSql, Array(row("appid"), row("planid"), row("channelid"), 1, TODAY))
            } else {
              val insertSql = "INSERT INTO statistics_base(app_id,plan_id,channel_id,launch_count,active_count,stat_date) VALUES(?,?,?,?,?,?)"
              JDBCutil.executeUpdate(connection, insertSql, Array(row("appid"), row("planid"), row("channelid"), 1, 1, TODAY))
            }
          }
          //end 计划基础数据更新和添加

          //start留存-旧设备才会有留存数据
          if(row("new") == 0){
            val planExistSqlRet = "SELECT * FROM statistics_retention WHERE app_id=? AND plan_id=? AND active_day=? AND retention_days=?"
            val statPrepRet = connection.prepareStatement(planExistSqlRet)
            val active_day = new SimpleDateFormat("yyyy-MM-dd").format(new SimpleDateFormat("yyyy-MM-dd").parse(row("activetime").toString))
            val last_launch_day = new SimpleDateFormat("yyyy-MM-dd").format(new SimpleDateFormat("yyyy-MM-dd").parse(row("launchtime").toString))
            //激活日期 和 最近上一次启动时间都不是当天的才会有留存数据
            if(active_day != TODAY && last_launch_day != TODAY){
              statPrepRet.setString(1, row("appid").toString)
              statPrepRet.setString(2, row("planid").toString)
              statPrepRet.setString(3, active_day)
              //计算留存天数 第二天启动则留存天数为2 第二天启动则留存天数为3 依此类推
              val retention_days = diffDays(active_day,TODAY) + 1
              if(retention_days > 1){
                statPrepRet.setInt(4, retention_days)
                val retRes = statPrepRet.executeQuery
                //查询留存记录表中是否存在记录 有记录更新 无记录写入
                if (retRes.next()) {
                  val updateSql = "UPDATE statistics_retention SET retention_count=retention_count+? WHERE app_id=? AND plan_id=? AND active_day=? AND retention_days=?"
                  JDBCutil.executeUpdate(connection, updateSql, Array(1, row("appid"), row("planid"), active_day, retention_days))
                } else {
                  val insertSql = "INSERT INTO statistics_retention(app_id,plan_id,channel_id,retention_count,retention_days,active_day) VALUES(?,?,?,?,?,?)"
                  JDBCutil.executeUpdate(connection, insertSql, Array(row("appid"),row("planid"), row("channelid"), 1, retention_days, active_day))
                }
              } else {
                throw new Exception("new字段为0旧设备,但retention_days 小于等于1")
              }
            }
          }
          //end留存
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

  private def regData(data: Iterator[Map[String, Any]]) = {
    val TODAY = this.getTODAY()
    try {
      val connection: Connection = JDBCutil.getConnection
      try {
        //println(data)
        //TODO 批量写入和更新基础统计数据
        val planExistSql = "SELECT * FROM statistics_base WHERE app_id=? AND plan_id=? AND stat_date=?"
        val statPrep = connection.prepareStatement(planExistSql)

        for (row <- data) {
          //start计划基础数据更新和添加
          statPrep.setString(1, row("appid").toString)
          statPrep.setString(2, row("planid").toString)
          statPrep.setString(3, TODAY)
          val res = statPrep.executeQuery
          if (res.next()) {
            //如果该计划已有当天的统计记录  则进行数据更新
            var updatePrep: PreparedStatement = null
            if (row("new") == 0) {
              val updateSql = "UPDATE statistics_base SET reg_count=reg_count+? WHERE app_id=? AND plan_id=? AND stat_date=?"
              JDBCutil.executeUpdate(connection, updateSql, Array(1, row("appid"), row("planid"), TODAY))
            } else {
              val updateSql = "UPDATE statistics_base SET launch_count=launch_count+?,active_count=active_count+? WHERE app_id=? AND plan_id=? AND stat_date=?"
              JDBCutil.executeUpdate(connection, updateSql, Array(1, 1, row("appid"), row("planid"), TODAY))
            }
          } else {
            //如果该计划没有当天的统计数据  则写入一条统计记录
            //这里的逻辑应该是执行不到的 因为激活上报接口一定会先写入一条记录
            if (row("new") == 0) {
              val insertSql = "INSERT INTO statistics_base(app_id,plan_id,channel_id,reg_count,stat_date) VALUES(?,?,?,?,?)"
              JDBCutil.executeUpdate(connection, insertSql, Array(row("appid"), row("planid"), row("channelid"), 1, TODAY))
            } else {
              val insertSql = "INSERT INTO statistics_base(app_id,plan_id,channel_id,launch_count,reg_count,stat_date) VALUES(?,?,?,?,?,?)"
              JDBCutil.executeUpdate(connection, insertSql, Array(row("appid"), row("planid"), row("channelid"), 1, 1, TODAY))
            }
          }
          //end 计划基础数据更新和添加
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
  private def payData(data:Iterator[Map[String,Any]]) = {
      val TODAY = this.getTODAY()
      //val connection = DriverManager.getConnection(prop.getProperty("mysql.url"), prop.getProperty("mysql.user"), prop.getProperty("mysql.password"))
      val connection: Connection = JDBCutil.getConnection
      try {
        val planExistSql = "SELECT * FROM statistics_pay WHERE app_id=? AND plan_id=? AND active_date=? AND pay_days=?"
        val statPrep = connection.prepareStatement(planExistSql)

        for (row <- data) {
          //start付费数据更新和添加
          val active_date = new SimpleDateFormat("yyyy-MM-dd").format(new SimpleDateFormat("yyyy-MM-dd").parse(row("activetime").toString))
          val pay_days = diffDays(active_date,TODAY) + 1  //付费天数 当天激活当天付费 pay_days 为1，第二天为2 依此类推
          statPrep.setString(1, row("appid").toString)
          statPrep.setString(2, row("planid").toString)
          statPrep.setString(3, active_date)
          statPrep.setInt(4, pay_days)
          val res = statPrep.executeQuery
          if (res.next()) {
            //更新付费统计
            val updateSql = "UPDATE statistics_pay SET pay_amount=pay_amount+?,pay_count=pay_count+1 WHERE app_id=? AND plan_id=? AND active_date=? AND pay_days=?"
            JDBCutil.executeUpdate(connection,updateSql,Array(row("amount"), row("appid"), row("planid"), active_date, pay_days))
          } else {
            //新增付费统计
            val insertSql = "INSERT INTO statistics_pay(app_id, plan_id, channel_id, pay_amount, pay_count, pay_days, active_date, pay_date) VALUES(?,?,?,?,?,?,?,?)"
            JDBCutil.executeUpdate(connection,insertSql,Array(row("appid"), row("planid"), row("channelid"), row("amount"), 1, pay_days, active_date, TODAY))
          }
          //end付费数据更新和添加
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
   * 提取对象obj的属性值 以map的形式返回
   * @param cc
   * @return
   */
  def getObjectProperties(cc: AnyRef) = {
    cc.getClass.getDeclaredFields.foldLeft(Map[String, String]()) {
      (a, f) => f.setAccessible(true)
      a + (f.getName -> f.get(cc).toString)
    }
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