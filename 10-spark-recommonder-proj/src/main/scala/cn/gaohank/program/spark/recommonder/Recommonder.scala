package cn.gaohank.program.spark.recommonder
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

class Recommonder {

}

object Recommonder {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setAppName("Recommonder").setMaster("local[2]")
        val sc = new SparkContext(conf)

        // 按照\t切分，得到lable和features
        //        val rddData = sc.textFile("e://traindata/000000_0")
        val rddData = sc.textFile("e://traindata/123.txt")
        //        println(rddData.collect().toBuffer)
        val splitRdd = rddData.map(_.split("\t"))

        //        println(splitRdd.collect().toBuffer)
        //        val features = splitRdd.map(x => x(1).split(";").map(_.split(":")(0)))
        val features=splitRdd.flatMap(_(1).split(";")).map(_.split(":")(0)).distinct()
        //        println(features.collect().toBuffer)
        // 生成字典表，转成map
        val dict = features.zipWithIndex().collectAsMap()

        // 构建labledpoint labledpoint包含lable和Vector
        val trainData = splitRdd.map(x => {
            // 由于逻辑回归算法中的lable只支持1.0和0.0
            val label = x(0) match {
                case "-1" => 0.0
                case "1" => 1.0
            }
            // 获取当前样本的每个特征在map中的下标，这些下标的位置都是非0
            val index = x.drop(1)(0).split(";").map(_.split(":")(0)).map(fe => {
                val index = dict.get(fe) match {
                    case Some(n) => n
                    case None => 0
                }
                index.toInt
            })
            //            val index = dict.get(fe)
            // 创建一个所有元素是1,0的数组，作为稀疏向量非0元素集合
            val vector = new SparseVector(dict.size, index, Array.fill(index.length)(1.0))
            // 构建lablepoint
            new LabeledPoint(label, vector)
        })

        // 训练模型（迭代次数和步长）
        val model = LogisticRegressionWithSGD.train(trainData, 10, 0, 0.1)
        // 得到权重这个weights下标和dict一样的
        val weights = model.weights.toArray
        // 将原来的字典表反转
        val map = dict.mapValues(x => {
            (x._2, x._1)
        })
        // 输出
        for (i <- 0 until weights.length) {
            val featureName = map.get(i) match {
                case Some(x) => x
                case None => ""
            }
        }

    }
}
