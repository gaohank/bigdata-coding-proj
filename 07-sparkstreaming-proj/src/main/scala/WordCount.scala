import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("WordCount").setMaster("local[2]")
        val sc = new SparkContext(conf)

        // 设置多少秒形成一个批次
        val ssc = new StreamingContext(sc,Seconds(3))
        val Dstream = ssc.socketTextStream("192.168.16.100",8888)
        val res = Dstream.flatMap(_.split(" ")).map((_,1)).reduceByKey(_+_)
        res.print()

        // 启动spark任务
        ssc.start()
        // 等待结束
        ssc.awaitTermination()

    }
}
