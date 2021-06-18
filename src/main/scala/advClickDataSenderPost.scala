

import java.security.MessageDigest
import java.text.SimpleDateFormat
import java.util
import java.util.Date

import scalaj.http.{Http, HttpRequest}

import scala.util._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import spray.json._
import DefaultJsonProtocol._

import scala.util.parsing.json.JSON

/**
 * 广告数据发生器
 */
object advClickDataSenderPost {
  def main(args: Array[String]): Unit = {
    val host = "http://192.168.207.136:8080/receive/clickhousePost"
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date))

    //POST 批量提交
    for (i <- 1 to 100) {
      val info = new ArrayBuffer[spray.json.JsValue]()
      for (j <- 1 to 10000) {
        val androidClickData: mutable.Map[String, String] = constructAndroidClickData
        val json = androidClickData.toMap.toJson
        info += (json)
      }
      val res = info.toArray.toJson.toString()
      //println(res)
      val request: HttpRequest = Http(host)
      //request.postForm(Seq("info" -> res)).proxy("127.0.0.1", 8888).asString
      request.postForm(Seq("info" -> res)).asString

      /*val androidClickData = new util.HashMap[String,Int]()
    androidClickData.put("a",1)
    androidClickData.put("b",2)
    val gson = new Gson()
    val json = gson.toJson(androidClickData)
    println(json)*/

      //val requestUrl = host + '?' + http_build_query(androidClickData)

      //POST方式
      /*val request: HttpRequest = Http(api)
    val randomData = sliceData.getRandomArray(60)

    for (i <- 0 to randomData.length) {
      for (j <- 0 to randomData(i)) {
        val info = constructAndroidDeviceClick()
        request.postForm(Seq("info" -> info)).proxy("127.0.0.1", 8888).asString
      }
      //发送一个单位的请求数量后延时等待1s
      Thread.sleep(1000)
    }*/

      //GET方式
      /*val request:HttpRequest = Http(requestUrl)
  val randomData = sliceData.getRandomArray(60)
  //print(randomData.foreach(println))
  val start = System.currentTimeMillis()
  println(start)
  for(i <- 0 until randomData.length){
    for(j <- 0 until randomData(i)) {
      request.proxy("127.0.0.1", 8888).asString
    }
    //发送一个单位的请求数量后延时等待1s
    //Thread.sleep(1000)
  }
  val end = System.currentTimeMillis()
  println(end)*/


      /**
       * 不使用http框架的原生发送请求方式
       */
      /*val httpGet = new HttpGet(requestUrl)
    //设置请求配置
    val proxyHost = new HttpHost("127.0.0.1", 8888)
    val requestConfig = RequestConfig.custom().setConnectTimeout(10000).setSocketTimeout(10000).setProxy(proxyHost).build() //设置代理
    //val requestConfig = RequestConfig.custom().setConnectTimeout(10000).setSocketTimeout(10000).build() //设置代理
    httpGet.setConfig(requestConfig)
    val httpclient = HttpClients.createDefault
    val response = httpclient.execute(httpGet)

    try {
      //println(response.getStatusLine)
      val entity = response.getEntity
      EntityUtils.consume(entity)
    } finally {
      response.close()
    }*/
      //延时1-10秒后发送设备信息
      /*Thread.sleep(1000)
  val androidDeviceData = constructAndroidDeviceData(androidClickData)
  println(androidClickData)
  println(androidDeviceData)
  val deviceApi = "http://192.168.207.136:8080/recive/launch"

  val request: HttpRequest = Http(deviceApi)
  request.postForm(androidDeviceData.toSeq).proxy("127.0.0.1", 8888).asString*/
    }
    println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date))
  }

  /**
   * 依据php的思想生成随机字符串
   *
   * @param args  生成的随机字符串的长度
   * @param flags 是否生成带字母的随机字符串
   *              true 带字母
   *              false 纯数字的随机字符串
   * @return
   */
  def getRandomString(args: Int, flags: Boolean = false): String = {
    var res = ""
    val srcString = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    var length = 0
    if (flags) {
      length = 32
    } else {
      length = 10
    }

    for (i <- 0 until args) {
      val index = Random.nextInt(length)
      res = res.concat(srcString.charAt(index).toString())
    }
    res
  }

  private def hashMD5(content: String): String = {
    val md5 = MessageDigest.getInstance("MD5")
    val encoded = md5.digest((content).getBytes)
    encoded.map("%02x".format(_)).mkString
  }

  /**
   * 模拟15位IMEI
   */
  def simulateIMEI(): String = {
    val imei = "86" + getRandomString(13)
    hashMD5(imei)
  }

  /**
   * 模拟MAC地址
   * 34:D7:12:9B:3A:89
   */
  def simulateMAC(): String = {
    var mac = ""
    for (i <- 0 until 6) {
      if (mac == "") {
        mac += getRandomString(2, true)
      } else {
        mac += ':' + getRandomString(2, true)
      }
    }
    hashMD5(mac)
  }

  /**
   * 模拟AndroidID
   * 7b5ca2d57178d2f1
   */
  def simulateAndroidID(): String = {
    val androidID = getRandomString(16, true).toLowerCase
    hashMD5(androidID)
  }

  /**
   * 模拟oaid
   * 97e7ef3f-e5f2-d0b8-ccfc-f79bbeaf4841
   * 有一定几率返回空字符串
   */
  def simulateOAID(): String = {
    var oaid = ""
    val random = Random.nextInt(100)
    if (random > 10) {
      oaid = getRandomString(8, true) + '-' + getRandomString(4, true) + '-' + getRandomString(4, true) + '-' + getRandomString(4, true) + '-' + getRandomString(12, true)
    }
    oaid.toLowerCase()
  }

  /**
   * 模拟IPv4地址
   * 113.246.105.229
   */
  def simulateIPv4(): String = {
    var ip = ""
    for (i <- 0 to 3) {
      if (ip == "") {
        ip += Random.nextInt(256)
      } else {
        ip += "." + Random.nextInt(256)
      }
    }
    ip
  }

  /**
   * 获取当前时间戳-毫秒
   */
  def getMillTimes(): String = {
    System.currentTimeMillis().toString
  }


  /**
   * 构造Android设备点击数据
   */
  def constructAndroidClickData(): mutable.Map[String, String] = {
    //Scala风格生成字符串
    val map = mutable.Map[String, String]()
    map += ("imei" -> simulateIMEI)
    map += ("mac" -> simulateMAC)
    map += ("androidid" -> simulateAndroidID)
    map += ("oaid" -> simulateOAID)
    map += ("ip" -> simulateIPv4)
    map += ("ts" -> getMillTimes)
    map += ("os" -> "0")
    val appid = Random.nextInt(10).toString
    val plan_id = Random.nextInt(1000).toString
    val channel_id = Random.nextInt(10).toString
    map += ("appid" -> appid)
    map += ("plan_id" -> plan_id)
    map += ("channel_id" -> channel_id)
    map
    /*val requestJson =  JSON.toJSONString(map,SerializerFeature.BeanToArray)
    requestJson*/

    //使用Java风格方式生成字符串
    /*val plan_id = Random.nextInt(1000).toString
    val channel_id = Random.nextInt(10).toString
    val params = new util.ArrayList[NameValuePair]()
    params.add(new BasicNameValuePair("appid", "123"))
    params.add(new BasicNameValuePair("imei", simulateIMEI))
    params.add(new BasicNameValuePair("mac", simulateMAC))
    params.add(new BasicNameValuePair("androidid", simulateAndroidID))
    params.add(new BasicNameValuePair("oaid", simulateOAID))
    params.add(new BasicNameValuePair("ip", simulateIPv4))
    params.add(new BasicNameValuePair("ts", getMillTimes))
    params.add(new BasicNameValuePair("os", "0"))
    params.add(new BasicNameValuePair("plan_id", plan_id))
    params.add(new BasicNameValuePair("channel_id", channel_id))

    val buildUrl =  new URIBuilder().addParameters(params).build().toString
    buildUrl*/
  }

  /**
   * 构造设备信息
   */
  def constructAndroidDeviceData(paramsMap: mutable.Map[String, String]): mutable.Map[String, String] = {
    val androidDevice = mutable.Map[String, String]()

    val properties = Array("imei", "oaid", "androidid", "mac", "ip")
    properties.foreach((item) => {
      //配置归因比例
      val random = Random.nextInt(100)
      if (random < 40) {
        androidDevice += (item -> paramsMap(item))
      } else {
        item match {
          case "imei" => androidDevice += ("imei" -> simulateIMEI)
          case "oaid" => androidDevice += ("oaid" -> simulateOAID)
          case "androidid" => androidDevice += ("androidid" -> simulateAndroidID)
          case "mac" => androidDevice += ("mac" -> simulateMAC)
          case "ip" => androidDevice += ("ip" -> simulateIPv4)
        }

      }
    })
    androidDevice += ("appid" -> paramsMap("appid"))
    androidDevice
  }

  /**
   * Scala实现 PHP 中的http_build_query()效果
   * 遍历 Map 形成akey=avalue&bkey=bvalue&ckey=cvalue形式的的字符串
   *
   * @param paramsMap key=value形式的 Map
   * @return
   */
  private def http_build_query(paramsMap: mutable.Map[String, String]): String = {
    var url = ""
    for ((key, value) <- paramsMap) {
      url += key + "=" + value + "&"
    }
    url = url.substring(0, url.length - 1)
    url
  }
}
