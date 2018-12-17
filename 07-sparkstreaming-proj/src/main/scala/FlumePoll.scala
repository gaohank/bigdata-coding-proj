import java.net.InetSocketAddress

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object FlumePoll {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("FlumePoll").setMaster("local[2]")
        val sc = new SparkContext(conf)

        val ssc = new StreamingContext(sc, Seconds(5))

        // 如果有多个flume，则new多个对象即可。
        val address = Seq(new InetSocketAddress("192.168.16.100", 1111))
        val flumeStream=FlumeUtils.createPollingStream(ssc, address, StorageLevel.MEMORY_ONLY)
        val tup = flumeStream.flatMap(x => new String(x.event.getBody().array).split(" ")).map((_, 1))
        val res = tup.reduceByKey(_+_);

        res.print()
        ssc.start()
        ssc.awaitTermination()
    }
}
