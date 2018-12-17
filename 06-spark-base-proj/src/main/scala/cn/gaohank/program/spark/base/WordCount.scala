package cn.gaohank.program.spark.base

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("WordCount").setMaster("local")
        val sc = new SparkContext(conf)

        val wordRdd = sc.textFile(WordCount.getClass.getClassLoader.getResource("wc.txt").getPath)
        println(wordRdd.collect().toBuffer)
        val words=wordRdd.flatMap(_.split(" "))
        println(words.collect().toBuffer)
        val pairs=words.map((_,1))

        //        val result=pairs.reduceByKey(_+_).sortBy(_._2,false)
        val result=pairs.reduceByKey((x, y) => x + y).sortBy(_._2,ascending = false)
        println(result.collect().toBuffer)

        val unit = pairs.aggregateByKey(0)(_+_, _+_).sortBy(_._2, ascending = false)
        println(unit.collect().toBuffer)
    }
}
