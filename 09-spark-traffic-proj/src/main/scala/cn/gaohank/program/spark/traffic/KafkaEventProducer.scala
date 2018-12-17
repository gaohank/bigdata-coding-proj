package cn.gaohank.program.spark.traffic

import java.util.Properties

import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}
import net.sf.json.JSONObject
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.{SparkConf, SparkContext}

object KafkaEventProducer {


    def main(args: Array[String]): Unit = {
        val topic = "car_event"
        val brokers = "192.168.16.100:9092"
        val props = new Properties()
        props.put("metadata.broker.list", brokers)
        props.put("serializer.class", "kafka.serializer.StringEncoder")

        val kafkaConfig = new ProducerConfig(props)
        val producer = new Producer[String, String](kafkaConfig)

        val sparkConf = new SparkConf().setAppName("Beijing traffic").setMaster("local[4]")
        val sc = new SparkContext(sparkConf)

        //    val filePath = ""
        val filePath = "e:/shuju.txt"

        val records = sc.textFile(filePath)
            .filter(!_.startsWith(";"))
            .map(_.split(",")).collect()

        // 加载决策树模型
        val model = DecisionTreeModel.load(sc, "D;//dfdfdfd");

        for(record <- records){
            // prepare event data
            val event = new JSONObject()
            event.put("camera_id", record(0))
            event.put("car_id", record(2))
            event.put("event_time", record(4))
            event.put("speed", record(6))
            event.put("road_id", record(13))

            // produce event message
            producer.send(new KeyedMessage[String, String](topic, event.toString))
            println("Message sent: " + event)

            Thread.sleep(200)
        }
    }
}
