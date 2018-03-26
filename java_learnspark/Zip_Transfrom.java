package java_learnspark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;

/**
 * Created by ycz on 2018/3/20.
 * 分区数和分区内元素必须相同，否则会跑出异常
 */
public class Zip_Transfrom {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Zip_Transfrom");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        List<Integer> data = Arrays.asList(5, 1, 1, 4, 4, 2, 2);
        JavaRDD<Integer> javaRDD = jsc.parallelize(data,3);
        List<Integer> data1 = Arrays.asList(3,2,12,5,6,1,7);
        JavaRDD<Integer> javaRDD1 = jsc.parallelize(data1,3);
        JavaPairRDD<Integer,Integer> zipRDD = javaRDD.zip(javaRDD1);
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" + zipRDD.collect());

    }
}
