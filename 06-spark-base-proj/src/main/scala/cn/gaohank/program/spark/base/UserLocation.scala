package cn.gaohank.program.spark.base

import org.apache.spark.{SparkConf, SparkContext}

object UserLocation {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("UserLocation").setMaster("local")
        val sc = new SparkContext(conf)

        val locationRdd = sc.textFile(UserLocation.getClass.getClassLoader.getResource("jztl.txt").getPath)
        val metaRdd = locationRdd.map { line =>
            val fields = line.split(",")
            val phone = fields(0)
            val et = fields(3)
            val time = fields(1)
            val timeLong = if (et == "1") -time.toLong else time.toLong

            (phone + "_" + fields(2), timeLong)
        }
        println(metaRdd.collect.toBuffer)

        val res = metaRdd.groupByKey().mapValues(_.sum)
        println(res.collect.toBuffer)

        // 接下来按手机号分组
        val rdd1 = res.map(t => {
            val phone_lac = t._1
            val phone = phone_lac.split("_")(0)
            val lac = phone_lac.split("_")(1)
            val time = t._2
            (phone, lac, time)
        })

        val rdd2 = rdd1.groupBy(_._1)
        println(rdd2.collect().toBuffer)

        // 排序
        val rdd3 = rdd2.mapValues(it => {
            // 操作的是集合，不是rdd
            it.toList.sortBy(_._3).reverse.take(1)
        })
        println(rdd3.collect().toBuffer)
    }
}
