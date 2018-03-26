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

/**
 * Created by ycz on 2018/3/20.
 */
public class Transform04 {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Transform04");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        List<Integer> data = Arrays.asList(1,1, 2, 4, 3, 5, 6, 7, 1, 2);
        JavaRDD<Integer> javaRDD = jsc.parallelize(data);

        JavaPairRDD<Integer,Integer> javaPairRDD = javaRDD.mapToPair(new PairFunction<Integer, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer integer) throws Exception {
                return new Tuple2<Integer, Integer>(integer,1);
            }
        });

//与 groupByKey() 不同，cogroup() 要 aggregate 两个或两个以上的 RDD。
        //System.out.println(javaPairRDD.collect());
        //javaPairRDD (1,1), (1,1), (2,1), (4,1), (3,1), (5,1), (6,1), (7,1), (1,1), (2,1)
        JavaPairRDD<Integer,Tuple2<Iterable<Integer>,Iterable<Integer>>> cogroupRDD = javaPairRDD.cogroup(javaPairRDD);
        //System.out.println(cogroupRDD.collect());
        //[(4,([1],[1])), (6,([1],[1])), (2,([1, 1],[1, 1])), (1,([1, 1, 1],[1, 1, 1])), (3,([1],[1])), (7,([1],[1])), (5,([1],[1]))]

        JavaPairRDD<Integer,Tuple2<Iterable<Integer>,Iterable<Integer>>> cogroupRDD3 = javaPairRDD.cogroup(javaPairRDD, new Partitioner() {
            @Override
            public int numPartitions() {
                return 2;
            }
            @Override
            public int getPartition(Object key) {
                return (key.toString()).hashCode()%numPartitions();
            }
        });
        //[(4,([1],[1])), (6,([1],[1])), (2,([1, 1],[1, 1])), (1,([1, 1, 1],[1, 1, 1])), (3,([1],[1])), (7,([1],[1])), (5,([1],[1]))]
        System.out.println(cogroupRDD3.collect());

    }
}
