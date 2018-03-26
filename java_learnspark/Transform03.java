package java_learnspark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

/**
 * Created by ycz on 2018/3/19.
 */
public class Transform03 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Transform03");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        /**
         * union
         * 合并
          */
        JavaRDD rdd1 = jsc.parallelize(Arrays.asList(1,2,3,4,5,6,7),2);
        JavaRDD rdd2 = jsc.parallelize(Arrays.asList(1,21,31,41,51,61,7),2);
        JavaRDD rdd3 = rdd1.union(rdd2);
        //System.out.println(rdd3.collect());

        /**
         * intersection
         * 交集
         */
        JavaRDD rdd4 = rdd1.intersection(rdd2);
        //System.out.println(rdd4.collect());

        /**
         * coalesce(numPartitions,shuffle)
         * shuffle=false是不能增加分区数量的
         * 当减少分区时，采用coalesce可以减少shuffle的损耗
         */
        JavaRDD rdd5 = rdd1.coalesce(3,true);
        /**
         * repartition(2)
         * 可以增加或者减少分区数量，底层调用coalesce（numpartitions,true）
         * 减少分区的时候建议使用coalesce，以免产生shuffle
         */
        JavaRDD rdd6 = rdd1.repartition(2);
        /**
         * cartesian
         * 笛卡尔积
         */
        JavaPairRDD rdd7 = rdd1.cartesian(rdd2);
        //System.out.println(rdd7.collect());
        /**
         * distinct
         */
        JavaRDD rdd8 = rdd3.distinct();
        System.out.println(rdd8.collect());
        JavaRDD rdd9 = rdd3.distinct(2);
        System.out.println(rdd9.collect());
    }

}
