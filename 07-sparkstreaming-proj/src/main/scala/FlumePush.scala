import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object FlumePush {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("FlumePush").setMaster("local[2]")
        val sc = new SparkContext(conf)

        val ssc = new StreamingContext(sc, Seconds(5))
        // Spark运行的地址，flume处理数据后把它下沉到这个ip上，ip只有一个，所以只有
        val flumeStream = FlumeUtils.createStream(ssc, "192.168.16.1", 1111)
        // 通过event.getBody()才能拿到真正的数据
        val words = flumeStream.flatMap(x => new String(x.event.getBody.array()).split(" ").map((_, 1)))
        val res = words.reduceByKey(_ + _)

        res.print()
        ssc.start()
        ssc.awaitTermination()
    }
}
