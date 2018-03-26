package java_learnspark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by ycz on 2018/3/21.
 */
public class GroupBy_Transform {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("GroupBy_Transform").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        List<String> data = Arrays.asList("a","b","a");
        JavaRDD rdd1 = jsc.parallelize(data);
        JavaPairRDD<Integer,Integer> javaPairRDD = rdd1.mapToPair(new PairFunction<Integer, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer integer) throws Exception {
                return new Tuple2<Integer, Integer>(integer,1);
            }
        });

    }
}
