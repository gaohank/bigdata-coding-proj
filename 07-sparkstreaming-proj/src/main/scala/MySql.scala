import java.util.Properties

import org.apache.spark.sql.{Row, SQLContext, types}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

object MySql {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("MySql").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)

        val studentRdd = sc.parallelize(Array("1 zhangsan 18", "2 lisi 22", "3 wangwu 66")).map(_.split(" "))
        val schema = StructType(
            List(
                StructField("id", IntegerType, true),
                StructField("name", StringType, true),
                StructField("age", IntegerType, true)
            )
        )

        // 将RDD映射到RowRdd上
        val rowRdd = studentRdd.map(x => Row(x(0).toInt, x(1), x(2).toInt))

        // 将schema应用到rowRdd上
        val studentDF = sqlContext.createDataFrame(rowRdd, schema)

        // 设置mysql的配置
        val prop = new Properties()
        prop.put("user", "root")
        prop.put("password", "root")

        // 将数据追加到mysql数据库
        // 需要安装链接mysql的驱动包
        studentDF.write.mode("append").jdbc("jdbc:mysql://192.168.16.100:3306/spark", "spark.students", prop)
        sc.stop()
    }
}
