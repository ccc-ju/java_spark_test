package java_learnspark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Serializable;
import scala.Tuple2;
import scala.tools.cmd.gen.AnyVals;

import java.util.*;

import static javafx.scene.input.KeyCode.F;

/**
 * Created by jiangtao on 2018/3/19.
 */
public class JavaUsualFunc {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local").setAppName("JavaUsualFunc");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        JavaRDD rdd1 = jsc.parallelize(Arrays.asList(1,2,3,4,5,6),2);

        /**
         * aggregate
         * param1：初始累加值，在分区内操作和分区操作时都进行累加
         * param2：函数，先对分区内合并
         * param3：函数，分区内合并后，在将他们的结果进行合并
         */
        Object aggregateResult = rdd1.aggregate(0, new Function2() {
            @Override
            public Object call(Object v1, Object v2) throws Exception {
                return Integer.parseInt(v1.toString())+Integer.parseInt(v2.toString());
            }
        }, new Function2() {
            @Override
            public Object call(Object v1, Object v2) throws Exception {
                return Integer.parseInt(v1.toString())+Integer.parseInt(v2.toString());
            }
        });
        //System.out.println(aggregateResult);//21
        /**
         * "","1"比较返回0,0和""比较返回0
         * "","23"比较返回1，最终结果返回1
         * 1，0拼接10或者01
         */
        JavaRDD rdd2 = jsc.parallelize(Arrays.asList("1","","23","32","0"),2);
        Object aggregateResult2 = rdd2.aggregate("", new Function2() {
            @Override
            public Object call(Object v1, Object v2) throws Exception {
                return Math.min(v1.toString().length(),v2.toString().length());
            }
        }, new Function2() {
            @Override
            public Object call(Object v1, Object v2) throws Exception {
                return v1.toString()+v2.toString();
            }
        });
       // System.out.println(aggregateResult2.toString()); // 01或者10 因为并行计算，不知道哪个先执行完
        /**
         * aggregate
         * 分区内 |ab     |cdf
         * 分区外在聚合 ||ab|cdf
         */
        JavaRDD rdd3 = jsc.parallelize(Arrays.asList("a","b","c","d","f"),2);
        Object aggregateResult3 = rdd3.aggregate("|", new Function2() {
            @Override
            public Object call(Object v1, Object v2) throws Exception {
                return v1.toString()+v2.toString();
            }
        }, new Function2() {
            @Override
            public Object call(Object v1, Object v2) throws Exception {
                return v1.toString()+v2.toString();
            }
        });
        //System.out.println(aggregateResult3.toString());

        /**
         * aggregateByKey
         */
        JavaRDD rdd4 = jsc.parallelize(Arrays.asList("pandas python","pandas","pandas","numpy","pandas","beautifulSoup","beautifulSoup"),2);

        JavaPairRDD wordCountResult = rdd4.flatMap(new FlatMapFunction() {
            @Override
            public Iterable call(Object o) throws Exception {
                return Arrays.asList(o.toString().split(" "));
            }
        }).mapToPair(new PairFunction() {
            @Override
            public Tuple2 call(Object o) throws Exception {
                return new Tuple2(o.toString(),1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
           @Override
           public Integer call(Integer i1,Integer i2) throws Exception {
               return i1+i2;
           }
       });
       //System.out.println(wordCountResult.collect());//[(beautifulSoup,2), (python,1), (pandas,4), (numpy,1)]

        JavaPairRDD aggregateByKeyResult = wordCountResult.aggregateByKey(5,2, new Function2() {
            @Override
            public Object call(Object v1, Object v2) throws Exception {
                return Math.max(Integer.parseInt(v1.toString()),Integer.parseInt(v2.toString()));
            }
        }, new Function2() {
            @Override
            public Object call(Object v1, Object v2) throws Exception {
                return Integer.parseInt(v1.toString())+Integer.parseInt(v1.toString());
            }
        });
        //System.out.println(aggregateByKeyResult.collect());

        /**
         * mapPartitions
         */
        List<Integer> data = Arrays.asList(1, 2, 4, 3, 5, 6, 7);

        JavaRDD<Integer> javaRDD = jsc.parallelize(data,2);
       //计算每个分区的合计
        JavaRDD<Integer> mapPartitionsRDD = javaRDD.mapPartitions(new FlatMapFunction<Iterator<Integer>, Integer>() {
            @Override
            public Iterable<Integer> call(Iterator<Integer> integerIterator) throws Exception {
                int isum = 0;
                while(integerIterator.hasNext()){
                    isum += integerIterator.next();
                }
                LinkedList<Integer> linkedList = new LinkedList<Integer>();
                linkedList.add(isum);
                return linkedList;    }
        });
        //System.out.println("mapPartitionsRDD~~~~~~~~~~~~~~~~~~~~~~" + mapPartitionsRDD.collect());

        /**
         * mapPartitionsWithIndex
         * java返回Iterator思路：数据封装到LinkedList中，返回LinkedList.iterator()
         */
        List<Integer> array1 = Arrays.asList(1, 2, 4, 3, 5, 6, 7);
//RDD有两个分区
        JavaRDD<Integer> rdd6 = jsc.parallelize(array1,2);
//分区index、元素值、元素编号输出
        JavaRDD<String> mapPartitionsWithIndexRDD = javaRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<Integer>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer v1, Iterator<Integer> v2) throws Exception {
                LinkedList<String> linkedList = new LinkedList<String>();
                while (v2.hasNext()){
                    linkedList.add(Integer.toString(v1) + "|" + v2.next().toString());
                }
                return linkedList.iterator();
            }
        },false);

        //System.out.println("mapPartitionsWithIndexRDD:" + mapPartitionsWithIndexRDD.collect());

        /**
         * foreachPartition
         * 不需要返回值，每个partition都要执行，这个可以用来写业务逻辑
         */

        /**
         *sortByKey
         * param1：函数，按照哪个规则去排序
         * param2：升序降序；true升序
         * param3：分区
         */
        List<Integer> array2 = Arrays.asList(1, 10, 4, 3, 5, 6, 7);
        JavaRDD<Integer> rdd7 = jsc.parallelize(array2,2);
        JavaRDD sortByResult = rdd7.sortBy(new Function<Integer, Object>() {
            @Override
            public Object call(Integer v1) throws Exception {
                return v1;
            }
        },true,4);
        //System.out.println(sortByResult.collect());

        /**
         * takeOrdered
         */
        //注意comparator需要序列化
        class TakeOrderedComparator implements Serializable,Comparator<Integer>{
            @Override
            public int compare(Integer o1, Integer o2) {
                return -o1.compareTo(o2);
            }
        }
        List<Integer> array3 = Arrays.asList(5, 1, 0, 4, 4, 2, 2);
        JavaRDD<Integer> rdd9 = jsc.parallelize(data, 3);
        System.out.println("takeOrdered-----1-------------" + rdd9.takeOrdered(2));
        List<Integer> list = rdd9.takeOrdered(2, new TakeOrderedComparator());
        System.out.println("takeOrdered----2--------------" + list);
    }
}

