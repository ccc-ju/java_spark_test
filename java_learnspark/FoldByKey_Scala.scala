package java_learnspark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ycz on 2018/3/21.
  * wordcount
  */
object FoldByKey_Scala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FoldByKey_Scala").setMaster("local")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(List("i","have","a","dream","i"),2)
    //val rdd2 = rdd1.map((x)=>(x,1)).foldByKey(0)(_+_)
    val rdd2 = rdd1.map((_,1)).foldByKey(0)(_+_)
    println(rdd2.collect.toBuffer)
  }
}
