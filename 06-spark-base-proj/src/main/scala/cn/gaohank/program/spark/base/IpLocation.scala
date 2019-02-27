package cn.gaohank.program.spark.base

import org.apache.spark.{SparkConf, SparkContext}

object IpLocation {
    def ip2Long(ip: String): Long = {
        val fragments = ip.split("[.]")
        var ipNum = 0L
        for (i <- 0 until fragments.length){
            ipNum =  fragments(i).toLong | ipNum << 8L
        }
        ipNum
    }

    def binarySearch(lines: Array[(String, String, String)], ip: Long) : Int = {
        var low = 0
        var high = lines.length - 1
        while (low <= high) {
            val middle = (low + high) / 2
            if ((ip >= lines(middle)._1.toLong) && (ip <= lines(middle)._2.toLong))
                return middle
            if (ip < lines(middle)._1.toLong)
                high = middle - 1
            else {
                low = middle + 1
            }
        }
        -1
    }

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("IpLocation").setMaster("local[1]")
        val sc = new SparkContext(conf)

        // 把规则库加载进来
        val ipRulesRdd = sc.textFile(IpLocation.getClass.getClassLoader.getResource("ip.txt").getPath).map(line => {
            // 转义， 因为竖线是正则
            val fields = line.split("\\|")
            val startNum = fields(2)
            val endNum = fields(3)
            val province = fields(6)

            (startNum, endNum, province)
        })

        // 汇聚到driver端
        val ipRules = ipRulesRdd.collect()

        // 广播出去
        val ipRulesBrodcast = sc.broadcast(ipRules)
        val rdd = sc.textFile(IpLocation.getClass.getClassLoader.getResource("http_format.txt").getPath).map(line => {
            val fields = line.split("\\|")
            fields(1)
        })

        val result = rdd.map(ip => {
            val ipNum = ip2Long(ip)

            // 返回一个索引
            val index = binarySearch(ipRules, ipNum)

            // info相当于找出的一条
            val info = ipRulesBrodcast.value(index)
            info
        })

        val res = result.map(t => {
            (t._3, 1)
        }).reduceByKey(_+_).sortBy(_._2, ascending = false).take(2)

        println(res.toBuffer)
        sc.stop()
    }
}
