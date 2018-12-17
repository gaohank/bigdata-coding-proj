import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object WindowFunc {
    def main(args: Array[String]): Unit = {
        LoggerLevels.setStreamingLogLevels()
        val conf = new SparkConf().setAppName("WindowFunc").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val ssc = new StreamingContext(sc, Seconds(5))

        val ds = ssc.socketTextStream("192.168.16.100", 1111)
        val tup = ds.flatMap(_.split(" ")).map((_, 1))
        val windowWc = tup.reduceByKeyAndWindow((a:Int, b:Int) => a + b, Seconds(30), Seconds(10))
        windowWc.print()
        ssc.start()
        ssc.awaitTermination()
    }
}
