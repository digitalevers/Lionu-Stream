object testJDBC {
  def main(args:Array[String]): Unit = {
    val conn = JDBCutil.getConnection
    val sql = "SELECT * FROM log_android_active WHERE oaid_md5=?"

    val rs = JDBCutil.executeQuery(conn,sql,Array("2a10298fe905dbff"))
    rs.foreach(println)
  }
}
