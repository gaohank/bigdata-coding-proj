package cn.gaohank.program.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SqlDemo {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("SqlDemo").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)

        val studentRdd = sc.textFile("e://spark.txt").map(line => {
            val fields = line.split(",")

            Student(fields(0).toInt, fields(1), fields(2).toInt)
        })

        import sqlContext.implicits._
        val df = studentRdd.toDF
        df.registerTempTable("student")
        df.createOrReplaceTempView("student1")
        val res = sqlContext.sql("select * from student where age > 20 order by age desc limit 3")
        res.write.json("e://out008")
    }
}

// 通过case class 传递Scema
case class Student(id:Int, name:String, age:Int)
