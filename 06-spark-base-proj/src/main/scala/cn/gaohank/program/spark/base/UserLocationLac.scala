package cn.gaohank.program.spark.base

import org.apache.spark.{SparkConf, SparkContext}

object UserLocationLac {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("UserLocationLac").setMaster("local[1]")
        val sc = new SparkContext(conf)

        val rdd = sc.textFile(UserLocation.getClass.getClassLoader.getResource("jztl.txt").getPath).map(line => {
            val fields = line.split(",")
            val phone = fields(0)
            val et = fields(3)
            val time = fields(1)
            val timeLong = if (et == "1") -time.toLong else time.toLong

            ((fields(0), fields(2)), timeLong)
        })
        println(rdd.collect.toBuffer)

        val rdd1 = rdd.reduceByKey(_+_).map(t => {
            val phone = t._1._1
            val lac = t._1._2
            val time = t._2
            (lac, (phone, time))
        })

        println(rdd1.collect.toBuffer)

        val rdd2 = sc.textFile(UserLocation.getClass.getClassLoader.getResource("lac_info.txt").getPath).map(line => {
            val f = line.split(",")

            // 返回一个元组
            (f(0), (f(1), f(2)))
        })

        val rdd3 = rdd1.join(rdd2)
        println(rdd3.collect().toBuffer)

        // 整理数组按照手机号分组，时间排序
        val rdd4 = rdd3.map(t => {
            val lac = t._1
            val phone = t._2._1._1
            val time = t._2._1._2
            val jd = t._2._2._1
            val wd = t._2._2._2
            (phone, lac, time, jd, wd)
        })
        println(rdd4.collect.toBuffer)

        val rdd5 = rdd4.groupBy(_._1)
        println(rdd5.collect.toBuffer)

        val rdd6 = rdd5.mapValues(it => {
            it.toList.sortBy(_._3).reverse.take(1)
        })

        rdd6.saveAsObjectFile("e:/result.out")
        sc.stop()
    }
}
