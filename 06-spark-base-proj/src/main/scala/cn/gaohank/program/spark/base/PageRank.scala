package cn.gaohank.program.spark.base

import org.apache.spark.{SparkConf, SparkContext}

object PageRank {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("PageRank").setMaster("local[1]")
        val sc = new SparkContext(conf)

        val lines = sc.textFile(PageRank.getClass.getClassLoader.getResource("pagerank.txt").getPath)

        // 生成临接表（1，（2,3,4,5））， （2，（1,5）），（3,1）
        val links = lines.map(t=>{
            val parts = t.split(" ")
            (parts(0), parts(1))
        }).distinct().groupByKey().cache()

        // (2, 1.0) (3, 1.0) (1, 1.0)
        var ranks = links.mapValues(v=>1.0)
        ranks.foreach(println)

        // (1, (2,3,4,5), 1.0), (2, (1,5), 1.0)
        val iters = 20;
        for (i <- 1 to iters) {
            val contribs = links.join(ranks).values.flatMap{
                case(urls, branks)=>
                    val size=urls.size
                    urls.map(url=>(url, branks / size))
            }

            ranks = contribs.reduceByKey(_+_).mapValues(0.15 + 0.85 * _)
        }

        val output  = ranks.collect()

        output.foreach(tup=>println(tup._1+"rank: "+tup._2))
        sc.stop()
    }
}
