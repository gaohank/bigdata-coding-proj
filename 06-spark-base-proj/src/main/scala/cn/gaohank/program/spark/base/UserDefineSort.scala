package cn.gaohank.program.spark.base

import org.apache.spark.{SparkConf, SparkContext}

// 定义一个case class，不用new
// 两两比较shuffle，要走网络，必须要进行Serializable
case class Goods(size:Int, price:Int) extends Ordered[Goods] with Serializable {
    override def compare(that: Goods): Int = {
        if (this.size == that.size) {
            // 谁大谁在前面
            that.price - this.price
        } else {
            // 谁小谁在前
            this.size - that.size
        }
    }
}
object UserDefineSort {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("UserDefineSort").setMaster("local[2]")
        val sc = new SparkContext(conf)

        val sortedRdd = sc.parallelize(List(("apple", 6, 5355), ("sumsung", 6, 4444), ("mi", 2, 1999), ("huawei", 5, 4567)))
        val rdd2 = sortedRdd.sortBy(_._2).sortBy(_._3)
        val rdd3 = sortedRdd.sortBy(x=>Goods(x._2, x._3))

        println(rdd2.collect().toBuffer)
        println(rdd3.collect().toBuffer)
        sc.stop()
    }
}
