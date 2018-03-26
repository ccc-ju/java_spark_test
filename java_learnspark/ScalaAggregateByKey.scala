package java_learnspark

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ycz on 2018/3/19.
  */
object ScalaAggregateByKey {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("ScalaAggregateByKey")
    val sc = new SparkContext(conf)

    //aggregateByKey
    val rdd1 = sc.parallelize(List(("cat",4),("dog",2),("mouse",3),("cat",1),("dog",3),("mouse",5)),2)

    val rdd2 = rdd1.aggregateByKey(0)(math.max(_,_),_+_)
    println(rdd2.collect.toBuffer)
    //val rdd2 = rdd1.aggregateByKey(0)((x,y)=>{Math.max(x,y)},(x,y)=>{x+y})
    //println(rdd2.collect.toBuffer)


  }
}
