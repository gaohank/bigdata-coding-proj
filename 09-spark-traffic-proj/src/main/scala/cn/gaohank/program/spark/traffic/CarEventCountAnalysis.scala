package cn.gaohank.program.spark.traffic

import java.text.SimpleDateFormat
import java.util.Calendar

import kafka.serializer.StringDecoder
import net.sf.json.JSONObject
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 用spark进行实时流处理
  */
object CarEventContAnalysis {
    def main(args: Array[String]): Unit = {
        val conf=new SparkConf().setAppName("CarEventContAnalysis").setMaster("local[2]")
        val sc =new SparkContext(conf)
        val ssc=new StreamingContext(sc,Seconds(5))
        //从kafka中拉取数据，直连的方式
        val topics=Set("car_event")
        val brokers="192.168.16.100:9092"
        //配置kafka的参数
        val kafkaParams=Map[String,String](
            "metadata.broker.list"->brokers,
            "serializer.class"->"kafka.serializer.StringEncoder"
        )
        //往哪一个redis库里写数据
        val dbindex=1
        //创建一个直连流
        val kafkaStream=KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topics)
        //把kafka中的数据取出来
        val events=kafkaStream.flatMap(line=>{

            //转化为object
            val data=JSONObject.fromObject(line._2)
            println(data)
            //返回，把他放到some中  人为的告诉它里面一定是有值的
            Some(data)
        })
        //从kafka中取出编号和速度数据并把他们形成（camera_id，（speed，1））
        val carSpeed=events.map(x=>(x.getString("camera_id"),x.getInt("speed"))).mapValues((x:Int)=>(x,1))
            //a:Tuple2[Int,Int] 前面的Int代表速度,后面的int代表1  //每隔10秒计算前20秒的数据（每隔10秒计算前20秒这个卡扣的总车辆和总速度）包括4个rdd
            .reduceByKeyAndWindow((a:Tuple2[Int,Int],b:Tuple2[Int,Int])=>{(a._1+b._1,a._2+b._2)},Seconds(20),Seconds(10))

//        carSpeed.map{case(key,value)=>(key,value._1/value._2.toFloat)}

        //把数据保存到redis数据库中
        carSpeed.foreachRDD(rdd => {
            rdd.foreachPartition(partitionOfRecords => {
                // 取到redis连接
                val jedis = RedisClient.pool.getResource
                // 拿到数据
                partitionOfRecords.foreach(pair => {
                    val camera_id = pair._1
                    val total = pair._2._1
                    val count = pair._2._2
                    //获当前时间
                    val now =Calendar.getInstance().getTime()
                    val minuteFormat=new SimpleDateFormat("HHmm")
                    val dayFormat=new SimpleDateFormat("yyyyMMdd")
                    val time=minuteFormat.format(now)
                    val day=dayFormat.format(now)

                    // 判断，加入车的数量不为0，保存到redis中
                    if (count != 0) {
                        jedis.select(dbindex)
                        // day + camera_id, time, total
                        jedis.hset(day + "_" + camera_id, time, total + "_" + count)
                        // 取出数据并打印
                        val fetchData = jedis.hget(day + "_" + camera_id, time)
                        println(fetchData)
                    }
                })

                RedisClient.pool.returnResource(jedis)
            })
        })
        ssc.start()
        ssc.awaitTermination()
    }
}