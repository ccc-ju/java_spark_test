package java_learnspark;

import org.apache.spark.Partitioner;
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
import java.util.Random;

/**
 * Created by ycz on 2018/3/20.
 */
public class FoldByKey_Transform {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("FoldByKey_Transform").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        List<Integer> data = Arrays.asList(1, 2, 4, 3, 5, 6, 7, 1, 2);
        JavaRDD<Integer> javaRDD = jsc.parallelize(data);
        final Random rand = new Random(10);
        JavaPairRDD<Integer,String> javaPairRDD = javaRDD.mapToPair(new PairFunction<Integer, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Integer integer) throws Exception {
                return new Tuple2<Integer, String>(integer,Integer.toString(rand.nextInt(10)));
            }
        });

        System.out.println(javaPairRDD.collect());
//[(1,3), (2,0), (4,3), (3,0), (5,6), (6,6), (7,7), (1,8), (2,1)]
        JavaPairRDD<Integer,String> foldByKeyRDD = javaPairRDD.foldByKey("X", new Function2<String, String, String>() {
            @Override
            public String call(String v1, String v2) throws Exception {
                return v1 + ":" + v2;
            }
        });
        //System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" + foldByKeyRDD.collect());

        JavaPairRDD<Integer,String> foldByKeyRDD1 = javaPairRDD.foldByKey("X", 2, new Function2<String, String, String>() {
            @Override
            public String call(String v1, String v2) throws Exception {
                return v1 + ":" + v2;
            }
        });
//[(4,X:3), (6,X:6), (2,X:0:1), (1,X:3:8), (3,X:0), (7,X:7), (5,X:6)]
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" + foldByKeyRDD1.collect());

        JavaPairRDD<Integer,String> foldByKeyRDD2 = javaPairRDD.foldByKey("X", new Partitioner() {
            @Override
            public int numPartitions() {        return 3;    }
            @Override
            public int getPartition(Object key) {
                return key.toString().hashCode()%numPartitions();
            }
        }, new Function2<String, String, String>() {
            @Override
            public String call(String v1, String v2) throws Exception {
                return v1 + ":" + v2;
            }
        });
        //System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" + foldByKeyRDD2.collect());

    }
}
