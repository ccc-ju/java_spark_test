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
public class FullOuterJoin_Transform {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("FullOuterJoin_Transform").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        List<Integer> data = Arrays.asList(1, 2,3, 5, 6, 7);
        List<Integer> data1 = Arrays.asList( 5, 6, 7);
        final Random random = new Random();
        JavaRDD<Integer> javaRDD = jsc.parallelize(data);
        JavaRDD<Integer> javaRDD1 = jsc.parallelize(data1);
        JavaPairRDD<Integer,Integer> javaPairRDD = javaRDD.mapToPair(new PairFunction<Integer, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer integer) throws Exception {
                return new Tuple2<Integer, Integer>(integer,random.nextInt(10));
            }
        });
        JavaPairRDD<Integer,Integer> javaPairRDD1 = javaRDD1.mapToPair(new PairFunction<Integer, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer integer) throws Exception {
                return new Tuple2<Integer, Integer>(integer,random.nextInt(10));
            }
        });

//全关联 [(1,2), (2,5), (3,4), (5,2), (6,5), (7,6)]
        System.out.println(javaPairRDD.collect());
        //[(5,2), (6,5), (7,4)]
        System.out.println(javaPairRDD1.collect());
        JavaPairRDD<Integer,Tuple2<Optional<Integer>,Optional<Integer>>> fullJoinRDD = javaPairRDD.fullOuterJoin(javaPairRDD1);
        System.out.println(fullJoinRDD.collect());
//[(1,(Optional.of(2),Optional.absent())), (6,(Optional.of(5),Optional.of(5))), (3,(Optional.of(4),Optional.absent())), (7,(Optional.of(6),Optional.of(4))), (5,(Optional.of(2),Optional.of(2))), (2,(Optional.of(5),Optional.absent()))]

        JavaPairRDD<Integer,Tuple2<Optional<Integer>,Optional<Integer>>> fullJoinRDD1 = javaPairRDD.fullOuterJoin(javaPairRDD,2);
        //System.out.println(fullJoinRDD1);

        JavaPairRDD<Integer,Tuple2<Optional<Integer>,Optional<Integer>>> fullJoinRDD2 = javaPairRDD.fullOuterJoin(javaPairRDD, new Partitioner() {
            @Override
            public int numPartitions() {        return 2;    }
            @Override
            public int getPartition(Object key) {   return (key.toString()).hashCode()%numPartitions();    }
        });
        //System.out.println(fullJoinRDD2);

    }
}
