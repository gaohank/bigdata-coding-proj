package cn.gaohank.program.spark.sql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object MySqlRead {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("MySql").setMaster("local[2]")
        val sc = new SparkContext(conf)
        val sqlContext = new SQLContext(sc)

        val jdbcDF=sqlContext.read.format("jdbc").options(Map("url"->"jdbc:mysql://192.168.16.100:3306/spark","driver"->"com.mysql.jdbc.Driver","dbtable"->"students","user"->"root","password"->"root")).load()
        jdbcDF.show()
    }
}
