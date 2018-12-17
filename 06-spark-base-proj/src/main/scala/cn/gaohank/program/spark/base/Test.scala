package cn.gaohank.program.spark.base

import org.apache.spark.{SparkConf, SparkContext}

object Test {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Test").setMaster("local")
        val sc = new SparkContext(conf)

        val names = List((1, 10), (2, 15), (3, 20), (1, 5))

        val nameRdd = sc.parallelize(names)
        val resultRdd = nameRdd.reduceByKey((a, b) => if (a > b) a else b)
        println(resultRdd.collect().toBuffer)
    }
}
