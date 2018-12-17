import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * 在lnuxi上开启端口8888 nc -lk 8888
  * 运行写的程序
  * 在nc上输入日志内容
  * 1124 jay   (jay,(1124 jay )) (jay,true)
  * 1124 mc
  * 1124 jj
  * 如果没有打印黑名单的内容就算成功
  */
object BlackList {
    def main(args: Array[String]): Unit = {
        LoggerLevels.setStreamingLogLevels()
        val conf = new SparkConf().setAppName("BlackList").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val ssc = new StreamingContext(sc, Seconds(5))

        // 黑名单数据准备，实际上黑名单一般都是动态的，例如在redis或者数据库中。
        // 可以实时更新
        // 在SparkStreaming进行处理时，每次都能够访问完整的信息
        val black=List(("lily",true))
        //需要sparkContext
        val blackRDD=ssc.sparkContext.parallelize(black)
        //监听h15上的9999端口
        val logs=ssc.socketTextStream("192.168.16.100", 9999)
        //分隔map
        val ds=logs.map { x => (x.split(" ")(1),x)}
        //创建transform操作
        // DStream离散流RDD->RDD，必须通过transform才能join
        val endDs =ds.transform( my=>{
            //左内连接：对于rdd和DStream连接     join是rdd和rdd连接
            val joinsRDD=my.leftOuterJoin(blackRDD)
            println(joinsRDD.collect().toBuffer)
            //过滤
            val endRDD=joinsRDD.filter(tuple=>{
                /**
                  * 举例说明：
                  * val cd=scores.getOrElse("Bob", 0)
                  * 如果scores包含Bob,那么返回Bob,如果不包含，那么返回0
                  */
                //意思是：tuple._2._2能get到值，返回值，如果不能得到值，返回false
                if (tuple._2._2.getOrElse(false)) {
                    false
                }else{
                    true
                }
            })
            //返回值
            endRDD.map(_._2._1)
        })
        //打印
        endDs.print()
        //开启
        ssc.start()
        //等待
        ssc.awaitTermination()

    }
}

