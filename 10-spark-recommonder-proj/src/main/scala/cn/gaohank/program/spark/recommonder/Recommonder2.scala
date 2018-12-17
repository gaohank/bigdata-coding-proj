package cn.gaohank.program.spark.recommonder
import java.io.PrintWriter

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.linalg.SparseVector
import org.apache.spark.mllib.regression.LabeledPoint

class Recommonder2 {

}
object Recommonder2 extends App{
    val conf=new SparkConf().setAppName("Recommonder").setMaster("local[2]")
    val sc=new  SparkContext(conf)
    //按照\t切分，得到lable和features字符串
    val data=sc.textFile("C:\\traindata\\000000_0").map(_.split("\t"))
    val features=data.flatMap(_.drop(1)(0).split(";")).map(_.split(":")(0)).distinct()
    //转成map
    val dict=features.zipWithIndex().collectAsMap()
    //构建labledpoint  labledpoint包含lable和Vector
    val trainData=data.map(x=>{
        //拿到lable，由于逻辑回归算法中lable只支持1.0和0.0
        val label=x(0) match{
            case "-1"=>0.0
            case "1"=>1.0
        }
        //获取当前样本的每个特征在map中的下标，这些下标的位置都是非0的值统一都是1.0
        val index=x.drop(1)(0).split(";").map(_.split(":")(0)).map(fe=>{
            val inde=dict.get(fe) match{
                case Some(n)=>n
                case None=>0
            }
            inde.toInt
        })
        //创建一个所有元素是1.0的数组，作为稀疏向量非0元素集合
        val vector=new SparseVector(dict.size,index,Array.fill(index.length)(1.0))
        //构建lablepoint
        new LabeledPoint(label,vector)


    })
    //训练模型(迭代次数和步长)
    val model =LogisticRegressionWithSGD.train(trainData,10,0.1)
    //得到权重这个weights下标和dirct一样的
    val weights=model.weights.toArray
    //将原来的字典表反转
    val map=dict.map(x=>{
        (x._2,x._1)
    })
    val pw=new PrintWriter("c://out12345678910")
    //输出
    for(i<-0 until weights.length){
        val featureName=map.get(i) match {
            case Some(x)=>x
            case None=>" "
        }
        val result=featureName +"\t"+weights(i)
        pw.write(result)
        pw.print()
    }
    pw.flush()
    pw.close()
}

