package cn.gaohank.program.spark.base

import org.apache.spark.{SparkConf, SparkContext}

object SparkPi {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("MySparkPi").setMaster("local[1]")
        val sc = new SparkContext(conf)

        val s = 100
        val n = 1000 * s
        val count = sc.parallelize(1 to n, s).map(i=>{
            // 返回一个大于等于0， 小于1的数
            def random:Double = Math.random()
            // 区间[-1, 1]
            val x = random * 2 - 1
            val y = random * 2 - 1
            if (x * x + y * y < 1) 1 else 0
        }).reduce(_+_)

        printf("n大概是：" + 4.0 * count/n)
        sc.stop()
    }
}
