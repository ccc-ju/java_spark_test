package java_learnspark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by ycz on 2018/3/20.
 * 两个rdd中的元素可以不相同，但是分区数量必须相同
 */
public class ZipPartitions_Transform {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("ZipPartitions_Transform");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        List<Integer> data = Arrays.asList(5, 1, 1, 4, 4, 2, 2);
        JavaRDD<Integer> javaRDD = jsc.parallelize(data, 3);
        List<Integer> data1 = Arrays.asList(3, 2, 12, 5, 6, 1);
        JavaRDD<Integer> javaRDD1 = jsc.parallelize(data1, 3);
        JavaRDD<String> zipPartitionsRDD = javaRDD.zipPartitions(javaRDD1, new FlatMapFunction2<Iterator<Integer>, Iterator<Integer>, String>() {
            @Override
            public Iterable<String> call(Iterator<Integer> integerIterator, Iterator<Integer> integerIterator2) throws Exception {
                LinkedList<String> linkedList = new LinkedList<String>();
                while (integerIterator.hasNext() && integerIterator2.hasNext())
                    linkedList.add(integerIterator.next().toString() + "_" + integerIterator2.next().toString());
                return linkedList;
            }
        });
        System.out.println(zipPartitionsRDD.collect());
    }


}
