object redisUtilTest {
  def main(args: Array[String]): Unit = {
    redisUtil.connect("192.168.207.136")
    for(i <- 0 to 10){
      val deviceExistInRedis = redisUtil.get("1-92acea49aed1abfb833759bba8319ba8").getOrElse("0").toInt
      println(deviceExistInRedis)
    }
  }
}
