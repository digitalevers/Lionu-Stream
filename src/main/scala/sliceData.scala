/**
 * 将一个大正整数“均匀”拆分到一个数组中的过程
 */
object sliceData {
  val unitsPerTime = 60*1        //每时间长度的单位数 比如可以是每小时分钟数或每小时秒数
  val timeLength = 1             //时长（小时数）

  val totalUnits = unitsPerTime * timeLength  //总单位数量
  val baseLimit = 5           //基准极限 进入递归过程的数据量增减幅度 比如基础数据10 进行一次随机增减后变成 15（10+5）或者5（10-5）
  val clicks: Array[Int] = new Array[Int](totalUnits)
  var totalAmount = 0

  /*def main(args: Array[String]): Unit = {
    totalAmount = scala.io.StdIn.readInt()
    randomProcess(totalAmount, baseLimit)
    clicks.foreach(x=>print(x+","))
    //println(clicks.sum)
  }*/

  def getRandomArray(totalAmountParam:Int):Array[Int] = {
    totalAmount = totalAmountParam
    randomProcess(totalAmount, baseLimit)
    clicks
  }

  /**
   * 随机过程
   * @param srcData 总数据量大小
   * @param baseLimit
   */
  private def randomProcess(srcData: Int, baseLimit: Int): Unit = {
    ////////////////////////////
    //粗略计算每个单位分摊的平均数值
    //如果小于等于0 说明数据量不足以分摊
    val perUnit = srcData / totalUnits

    if (perUnit <= 0) {
      val sliceMount = Math.abs(srcData / baseLimit).toInt

      for (i <- 1 to sliceMount) {
        val index = util.Random.nextInt(totalUnits)
        if (srcData < 0) {
          //压缩数据 （做出判断防止出现负数）
          if ((clicks(index) - baseLimit) >= 0) {
            clicks(index) -= baseLimit
          }
        } else {
          //增加数据
          clicks(index) += baseLimit
        }
      }
    } else {
      for (i <- 0 until totalUnits) {
        clicks(i) = util.Random.nextInt(perUnit * 2 + 1)
      }
    }
    ///////////////
    if (totalAmount != clicks.sum) {
      if (totalAmount - clicks.sum > baseLimit || totalAmount - clicks.sum < -baseLimit) {
        //继续递归过程
        randomProcess(totalAmount - clicks.sum, baseLimit)
      } else {
        randomProcess(totalAmount - clicks.sum, 1)
      }
    }
  }

  /////////end process
}