package java_learnspark;

import com.google.common.base.Optional;
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
public class LeftOuterJoin_Transform {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("LeftOuterJoin_Transform").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        List<Integer> data = Arrays.asList(1, 2, 4, 3, 5, 6, 7);
        final Random random = new Random();
        JavaRDD<Integer> javaRDD = jsc.parallelize(data);
        JavaPairRDD<Integer,Integer> javaPairRDD = javaRDD.mapToPair(new PairFunction<Integer, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer integer) throws Exception {
                return new Tuple2<Integer, Integer>(integer,random.nextInt(10));
            }
        });

//左关联
        JavaPairRDD<Integer,Tuple2<Integer,Optional<Integer>>> leftJoinRDD = javaPairRDD.leftOuterJoin(javaPairRDD);
        System.out.println(leftJoinRDD.collect());

        JavaPairRDD<Integer,Tuple2<Integer,Optional<Integer>>> leftJoinRDD1 = javaPairRDD.leftOuterJoin(javaPairRDD,2);
        //System.out.println(leftJoinRDD1);

        JavaPairRDD<Integer,Tuple2<Integer,Optional<Integer>>> leftJoinRDD2 = javaPairRDD.leftOuterJoin(javaPairRDD, new Partitioner() {
            @Override
            public int numPartitions() {        return 2;    }
            @Override
            public int getPartition(Object key) { return (key.toString()).hashCode()%numPartitions();
            }
        });
        //System.out.println(leftJoinRDD2);


    }
}
