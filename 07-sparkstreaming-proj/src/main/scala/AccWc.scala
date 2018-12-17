import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object AccWc {
    // 当前批次某个单词出现的次数
    // Option[Int]：以前的累加值
    val updateFunc = (iter:Iterator[(String, Seq[Int], Option[Int])]) => {
        iter.flatMap({
            // Some代表可以取到值
            case (x, y, z) => Some(y.sum + z.getOrElse(0)).map(m => (x, m))
        })
    }

    val updateFunc1 = (iter:Iterator[(String, Seq[Int], Option[Int])]) => {
        iter.flatMap(it => Some(it._2.sum + it._3.getOrElse(0)).map(x => (it._1, x)))
    }

    def main(args: Array[String]): Unit = {
        LoggerLevels.setStreamingLogLevels()
        val conf = new SparkConf().setAppName("AccWc").setMaster("local[2]")
        val sc = new SparkContext(conf)

        // 实时计算一定要做CheckPoint
        sc.setCheckpointDir("e://ck110")

        val ssc = new StreamingContext(sc, Seconds(5))

        // 离散化的流对象，DStream
        val ds = ssc.socketTextStream("192.168.16.100", 8888)
        // 使用默认的分区方式
        val res = ds.flatMap(_.split(" ")).map((_, 1)).updateStateByKey(updateFunc,
            new HashPartitioner(sc.defaultParallelism), true)

        res.print()
        ssc.start()
        ssc.awaitTermination()
    }
}
