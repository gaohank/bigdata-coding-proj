package cn.gaohank.program.spark.base

import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}

object UrlShort {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("UrlShort").setMaster("local[1]")
        val sc = new SparkContext(conf)

        val hadoopRdd = sc.textFile(UrlShort.getClass.getClassLoader.getResource("url.txt").getPath)
        val rdd = hadoopRdd.map { line =>
            val f = line.split("\t")
            (f(1), 1)
        }

        val tuples = hadoopRdd.map { line =>
            val f = line.split("\t")
            (f(0), f(1))
        }.reduceByKey(_ + _)
        println(tuples.collect().head)

        val rdd2 = rdd.reduceByKey(_+_)

        val rdd3 = rdd2.map(t => {
            val url = t._1
            // 调用java的URL方法
            val host = new URL(url).getHost
            (host, url, t._2)
        })

        val rdd4 = rdd3.groupBy(_._1).mapValues(it => {
            it.toList.sortBy(_._3).reverse.take(3)
        })

        println(rdd4.collect.toBuffer)
        sc.stop()
    }
}
