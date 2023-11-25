import ResultJsonProtocol._
import spray.json.{JsonFormat, enrichAny}



object testJDBC {
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
    case class User(a:Int,b:Int)
    implicit val userFormat: JsonFormat[User] = jsonFormat2(User)
    val map = Map('a'->1,'b'->2)
    val json = map.toJson.compactPrint
    print(json)

  }
}
