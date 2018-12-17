package cn.gaohank.program.spark.base

import java.net.URL

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

object UrlShortTable {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("UserLocation").setMaster("local[1]")
        val sc = new SparkContext(conf)

        // 创建字典表
        val arr = Array("sport.sina.cn", "car.sina.cn", "game.sina.cn")

        val hadoopRdd = sc.textFile("e://url.log")
        val rdd = hadoopRdd.map(line => {
            val f = line.split("\t")
            (f(1), 1)
        })

        val rdd2 = rdd.reduceByKey(_+_)

        val rdd3 = rdd2.map(t => {
            val url = t._1
            // 调用java的URL方法
            val host = new URL(url).getHost
            (host, (url, t._2))
        })

        val hosts = rdd3.map(_._1).distinct().collect()
        val np = new NewPartition(hosts)
        rdd3.partitionBy(np).saveAsTextFile("e:urlOuts")

        sc.stop()
    }
}

class NewPartition(hosts:Array[String]) extends Partitioner {
    // 完成哪个key对应哪个分区
    val partMap = new mutable.HashMap[String, Int]()
    var count = 0
    for (i <- hosts) {
        partMap += (i->count)
        count+=1
    }

    override def numPartitions: Int = hosts.length

    override def getPartition(key: Any): Int = {
        partMap.getOrElse(key.toString, 5000)
    }
}
