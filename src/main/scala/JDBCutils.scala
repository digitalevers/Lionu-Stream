import com.alibaba.druid.pool.DruidDataSourceFactory

import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.Properties
import javax.sql.DataSource

object JDBCutil {
  //初始化连接池
  var dataSource: DataSource = init()

  //初始化连接池方法
  def init(): DataSource = {
    //读取配置文件
    val prop = new Properties();
    // 使用ClassLoader加载properties配置文件生成对应的输入流
    val in = getClass.getClassLoader().getResourceAsStream("application.properties");
    // 使用properties对象加载输入流
    prop.load(in)

    val properties = new Properties()
    properties.setProperty("driverClassName", "com.mysql.cj.jdbc.Driver")
    properties.setProperty("url", prop.getProperty("mysql.url"))
    properties.setProperty("username", prop.getProperty("mysql.user"))
    properties.setProperty("password", prop.getProperty("mysql.password"))
    properties.setProperty("maxActive", "50")
    DruidDataSourceFactory.createDataSource(properties)
  }

  //获取mysql连接
  def getConnection: Connection = {
    dataSource.getConnection
  }

  //执行SQL语句，单条数据插入
  def executeUpdate(connection: Connection, sql: String, params: Array[Any]): Int = {
    var rtn = 0
    var pstmt: PreparedStatement = null
    try {
      connection.setAutoCommit(false)
      pstmt = connection.prepareStatement(sql)
      if (params != null && params.length > 0) {
        for (i <- params.indices) {
          pstmt.setObject(i + 1, params(i))
        }
      }
      rtn = pstmt.executeUpdate()
      connection.commit()
      pstmt.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    rtn
  }

  /**
   *   查找记录
   *   查询到 以数组形式返回第一行记录
   *   未查询到返回 null
   */

  def executeQuery(connection: Connection, sql: String, params: Array[Any]): Array[Any] = {
    var rs:ResultSet = null
    var queryResult:Array[Any] = null
    var pstmt: PreparedStatement = null
    try {
      pstmt = connection.prepareStatement(sql)
      for (i <- params.indices) {
        pstmt.setObject(i + 1, params(i))
      }
      rs = pstmt.executeQuery()
      if(rs.next()){
        queryResult = Array(rs.getArray(0))
      }
      pstmt.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    queryResult
  }

  //判断记录是否存在
  def isExist(connection: Connection, sql: String, params: Array[Any]): Boolean = {
    var flag: Boolean = false
    var pstmt: PreparedStatement = null
    try {
      pstmt = connection.prepareStatement(sql)
      for (i <- params.indices) {
        pstmt.setObject(i + 1, params(i))
      }
      flag = pstmt.executeQuery().next()
      pstmt.close()
    } catch {
      case e: Exception => e.printStackTrace()
    }
    flag
  }

}
