package java_learnspark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by ycz on 2018/3/20.
 */
public class FoldByKey_WordCount {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("FoldByKey_WordCount");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD rdd1 = jsc.parallelize(Arrays.asList("word","dream","have","python","word"));
        JavaPairRDD rdd2 = rdd1.mapToPair(new PairFunction<String,String,Integer>() {
            @Override
            public Tuple2<String,Integer> call(String o) throws Exception {
                return new Tuple2<String,Integer>(o,1);
            }
        });
        JavaPairRDD rdd3 = rdd2.foldByKey(0, new Function2<Integer,Integer,Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1+v2;
            }
        });
        System.out.println(rdd3.collect());
    }
}
