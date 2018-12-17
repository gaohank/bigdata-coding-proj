
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object StructType1 {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("SqlDemo").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)

        val studentRdd = sc.textFile("e://spark.txt").map(_.split(","))
        // 结构类型，直接指定每个字段的schema
        val schema = StructType (
            List(
                StructField("id", IntegerType, true),
                StructField("name", StringType, true),
                StructField("age", IntegerType, true)
            ) // 是否允许为空
        )

        val rowRdd = studentRdd.map(x => Row(x(0).toInt, x(1), x(2).toInt))
        val studentDF = sqlContext.createDataFrame(rowRdd, schema)
        studentDF.registerTempTable("student")

        // 默认保存格式是parquet File
        // 将数据存储成列式格式，以方便对其高效的压缩和编码，且使用更少的操作，压缩的更小
        // 还可以指定json格式
        val df = sqlContext.sql("select * from student order by age desc limit 2")
        df.write.save("e://out009")
//        df.write.text("e://out010")// 只要一列，一般用默认或者json就可以了
        sqlContext.read.load("e://out009")
//        sqlContext.read.load("e://out010", "text")
    }
}
