package java_learnspark;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Created by ycz on 2018/3/20.
 */
public class Transform05 {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Transform04");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        List<Integer> data = Arrays.asList(1, 1,2, 4, 3, 5, 6, 7);
        final Random random = new Random();
        JavaRDD<Integer> javaRDD = jsc.parallelize(data);
        JavaPairRDD<Integer,Integer> javaPairRDD = javaRDD.mapToPair(new PairFunction<Integer, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer integer) throws Exception {
                return new Tuple2<Integer, Integer>(integer,random.nextInt(10));
            }
        });
        //[(1,7), (2,9), (4,5), (3,7), (5,9), (6,5), (7,2)]
        System.out.println(javaPairRDD.collect());
        JavaPairRDD<Integer,Tuple2<Integer,Integer>> joinRDD = javaPairRDD.join(javaPairRDD);
        //System.out.println(joinRDD.collect());

        JavaPairRDD<Integer,Tuple2<Integer,Integer>> joinRDD2 = javaPairRDD.join(javaPairRDD,2);
        System.out.println(joinRDD2.collect());

        JavaPairRDD<Integer,Tuple2<Integer,Integer>> joinRDD3 = javaPairRDD.join(javaPairRDD, new Partitioner() {
            @Override
            public int numPartitions() {
                return 2;
            }
            @Override
            public int getPartition(Object key) {
                return (key.toString()).hashCode()%numPartitions();
            }
        });
        //System.out.println(joinRDD3.collect());


    }
}
