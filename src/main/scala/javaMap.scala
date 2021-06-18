import java.net.SocketTimeoutException
import java.sql.Time
import java.text.SimpleDateFormat
import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime, ZoneId}
import java.util.{Date, HashMap}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks

object javaMap {
  def main(args: Array[String]): Unit = {
    /*val coursesMap = new HashMap[Integer,String] ();
    coursesMap.put(1, "C");
    coursesMap.put(2, "C++");
    coursesMap.put(3, "Java");
    coursesMap.put(4, "Spring Framework");
    coursesMap.put(5, "Hibernate ORM framework");*/
    //println(coursesMap)

    /*val map1 = mutable.Map[String,Int]()
    map1 += ("C"->1)
    map1 += ("C++"->2)
    map1 += ("Java"->3)
    map1 += ("Spring Framework"->4)
    map1 += ("Hibernate ORM framework"->5)

    map1.map(t=>{
      map1(t._1) =  t._2 * 2
    })
    println(map1)*/
    /*val arr = new ArrayBuffer[Int]()
    arr += (5)
    println(arr)*/
    /*val sqls = mutable.LinkedHashMap[String,String](
      "imei"->"SELECT  * FROM test.array_test WHERE imei_md5=?",
      "oaid"->"SELECT  * FROM test.array_test WHERE oaid=?",
      "androidid"->"SELECT  * FROM test.array_test WHERE androidid_md5=?",
      "mac"->"SELECT  * FROM test.array_test WHERE mac_md5=?",
      "ip"->"SELECT  * FROM test.array_test WHERE ip=?"
    )

    val loop = new Breaks;

    for ((k, sql) <- sqls) {
      println(sql)
      if (k == "mac") {
        loop.breakable {
          loop.break()
        }
      }
    }*/

    /*try{
      redisUtil.connect("192.168.207.136")
    } catch {
      case ex:SocketTimeoutException=>{
        println("连接超时")
      }
      case ex:Exception=>{
        println(ex.getMessage)
      }
    }
    println(redisUtil.set("abc","1"))*/

    //println(test(8))
  }

  def test(param:Int):Int = {
    if(param > 10){
      1
    } else {
      2
    }
  }
}

