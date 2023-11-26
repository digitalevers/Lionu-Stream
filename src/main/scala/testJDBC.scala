import ResultJsonProtocol._
import sparkSteamReConsitution.{isNewDeviceInMySQL, isNewDeviceInRedis}
import spray.json.{JsonFormat, JsonParser, enrichAny}

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}



object testJDBC {
  private val NOW = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date())
  private val TODAY = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
  def main(args:Array[String]): Unit = {
//    try{
//      val conn = JDBCutil.getConnection
//      val sql = "SELECT * FROM log_android_active WHERE oaid_md5=? ORDER BY active_time DESC"
//
//      val rs = JDBCutil.executeQuery(conn, sql, Array("2a10298fe905dbff"))
//      println(rs)
//    }catch {
//      case ex: Exception => {
//        println(ex.getMessage)
//      }
//    }
//    case class User(a:Int,b:Int)
//    implicit val userFormat: JsonFormat[User] = jsonFormat2(User)
//    val map = Map('a'->1,'b'->2)
//    val json = map.toJson.compactPrint
//    print(json)

//    val a = 1
//    val b = 1
//    var c = 3
//    var d = 4
//    var sjon = s"""{"activetime":"${NOW}","launchtime":"${NOW}","planid":"${a}","channelid":"${b}"}"""
//    var map = JsonParser(sjon).convertTo[Map[String,String]]
//    println(map)

    val date = new SimpleDateFormat("yyyy-MM-dd").format(new SimpleDateFormat("yyyy-MM-dd").parse("2023-11-22 10:13:08"))


    println(date)
  }

  def fun1() = {
    //println("fun1")
    123
  }

  def fun2() = {
    println("fun2")
    s"""{"a":1,"b":2}"""
  }
}
