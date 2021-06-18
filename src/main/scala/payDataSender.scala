
import scalaj.http.{Http, HttpRequest}

import java.security.MessageDigest
import java.util.Properties
import scala.collection.mutable
import scala.util._


/**
 * 付费数据发生器
 */
object payDataSender {
  def main(args: Array[String]): Unit = {
      val prop = new Properties();
      val in = payDataSender.getClass.getClassLoader().getResourceAsStream("application.properties");
      prop.load(in)

      val host = prop.getProperty("phpApiUrl")+"/receive/pay"
      val androidPayData = constructAndroidPayData
      val request: HttpRequest = Http(host)
      if(prop.getProperty("proxy.open") == "1") {
        request.postForm(androidPayData.toSeq).proxy("127.0.0.1", 8888).asString
      } else {
        request.postForm(androidPayData.toSeq).asString
      }
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
   * 构造Android设备付费数据
   */
  def constructAndroidPayData(): mutable.Map[String, String] = {
    //Scala风格生成字符串
    val map = mutable.Map[String, String]()
    map += ("imei" -> "eae5ab42f65c8553e40419948800ef3d") //simulateIMEI
    map += ("mac" -> simulateMAC)
    map += ("androidid" -> simulateAndroidID)
    map += ("oaid" -> simulateOAID)
    map += ("ip" -> simulateIPv4)
    map += ("ts" -> getMillTimes)
    map += ("os" -> "0")
    map += ("appid" -> "5")     //appid
    map += ("amount" -> "20")   //付费金额
    map
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
