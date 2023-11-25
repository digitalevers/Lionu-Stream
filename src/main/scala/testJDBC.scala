

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
    val map = Map('a'->1)
    // 使用ClassLoader加载properties配置文件生成对应的输入流
    val in = getClass.getClassLoader().getResourceAsStream("application.properties")

    // 使用properties对象加载输入流
    //prop.load(in)
    //redisUtil.connect(prop.getProperty("redis.server"))
    //val deviceExistInRedis = redisUtil.get("5-imei").getOrElse(null)

    //print(deviceExistInRedis)
  }
}
