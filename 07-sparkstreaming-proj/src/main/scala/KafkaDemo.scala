import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object KafkaDemo {
    val updateFunc = (iter:Iterator[(String, Seq[Int], Option[Int])]) => {
        iter.flatMap(it => Some(it._2.sum + it._3.getOrElse(0)).map(x => (it._1, x)))
    }

    def main(args: Array[String]): Unit = {
        LoggerLevels.setStreamingLogLevels()
        val Array(zkQuorum, group, topics, numThreads) = args;
        val conf = new SparkConf().setAppName("KafkaDemo").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val ssc = new StreamingContext(sc, Seconds(5))

//        ssc.checkpoint("e://checkpoint")
        sc.setCheckpointDir("e://checkpoint")
        val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
        val ds = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap, StorageLevel.MEMORY_AND_DISK)

        val words = ds.map(_._2).flatMap(_.split(" "))
        val res = words.map((_, 1)).updateStateByKey(updateFunc,
            new HashPartitioner(sc.defaultParallelism), true)

        println(res)
        ssc.start()
        ssc.awaitTermination()
    }
}
