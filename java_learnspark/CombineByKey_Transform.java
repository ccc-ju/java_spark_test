package java_learnspark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by ycz on 2018/3/20.
 */
public class CombineByKey_Transform {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("CombineByKey_Transform").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        List<Integer> data = Arrays.asList(1, 2, 4, 3, 5, 6, 7, 1, 2);
        JavaRDD<Integer> javaRDD = jsc.parallelize(data);
//转化为pairRDD
        JavaPairRDD<Integer,Integer> javaPairRDD = javaRDD.mapToPair(new PairFunction<Integer, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer integer) throws Exception {
                return new Tuple2<Integer, Integer>(integer,1);
            }
        });

        JavaPairRDD<Integer,String> combineByKeyRDD = javaPairRDD.combineByKey(new Function<Integer, String>() {
            @Override
            public String call(Integer v1) throws Exception {
                return v1 + " :createCombiner: ";
            }
        }, new Function2<String, Integer, String>() {
            @Override
            public String call(String v1, Integer v2) throws Exception {
                return v1 + " :mergeValue: " + v2;
            }
        }, new Function2<String, String, String>() {
            @Override
            public String call(String v1, String v2) throws Exception {
                return v1 + " :mergeCombiners: " + v2;
            }
        });
        System.out.println("result:" + combineByKeyRDD.collect());


    }
}
