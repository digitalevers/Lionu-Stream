import ResultJsonProtocol._
import sparkSteamReConsitution.{isNewDeviceInMySQL, isNewDeviceInRedis}
import spray.json.{JsonFormat, JsonParser, enrichAny}

import java.lang.reflect.Field
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

class User{
  private val name ="lucy"
  private val age = 20

  def getAge(): Unit = {

  }
}

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
    val user = new User
    //println(this.getCCParams(user))
    println(diffDays("2023-11-27 12:11:22","2023-11-28:02:53:23"))
  }

  private def diffDays(startDate: String, endDate: String): Int = {
    val dft = new SimpleDateFormat("yyyy-MM-dd")

    val start = dft.parse(startDate)
    val end = dft.parse(endDate)
    val starTime = start.getTime
    val endTime = end.getTime
    val num = ((endTime - starTime) / 1000).toInt //时间戳相差的毫秒数
    //System.out.println("相差天数为：" + num / 24 / 60 / 60 / 1000) //除以一天的毫秒数
    num / 24 / 60 / 60

  }

  def fun1(a: Map[String, String], f: Field) = {
    //println("fun1")

  }


  def getCCParams(cc: AnyRef) = {

    cc.getClass.getDeclaredFields.foldLeft(Map[String, String]())((x,y)=>{
      y.setAccessible(true)
      x + (y.getName -> y.get(cc).toString)
    })

    cc.getClass.getDeclaredFields.foldLeft(Map[String, String]()) {
      (x,y) => y.setAccessible(true)
      x + (y.getName -> y.get(cc).toString)
    }
  }


  def fun2() = {
    println("fun2")
    s"""{"a":1,"b":2}"""
  }
}
