package java_learnspark;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

/**
 * Created by ycz on 2018/3/19.
 */
public class JavaSparkInterface {
    public static void main(String[] args){
        /**
         * 初始化sparkCore的context javaSparkContext
         * 初始化sparkStreaming的context
         * JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(2000));
         * 初始化SQLContext
         *  val sqlContext = new SQLContext(sc)
         *  SQLContext sqlContext = SQLContext.getOrCreate(rdd.context());
         */

        SparkConf conf = new SparkConf().setAppName("java_interface").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        //并行集合生成JavaRDD
        JavaRDD lines = jsc.parallelize(Arrays.asList("pandas python","","numpy","pandas","beautifulSoup"));
        //System.out.println(lines.collect());

        //transform算子 filter
        JavaRDD filterResult = lines.filter(new Function() {
            @Override
            public Object call(Object o) throws Exception {
                return o.toString().contains("pandas");
            }
        });
        //filter lamda
        //JavaRDD filterResult = lines.filter(s -> s.contains("pandas"));

        //System.out.println(filterResult.collect());

        //map
        JavaRDD mapResult = lines.map(new Function() {
            @Override
            public Object call(Object o) throws Exception {
                return o.toString().concat("newTail");
            }
        });
        //System.out.println(mapResult.collect());

        /**
         * 键值对的RDD统称为pair rdd，将普通的rdd转换为pairrdd通过mapToPair方法
         */
        JavaPairRDD<String,Integer> mapToPairResult = lines.mapToPair(new PairFunction() {
            @Override
            public Tuple2<String,Integer> call(Object o) throws Exception {
                Tuple2 tuple2 = new Tuple2(o.toString(),1);
                //System.out.println(tuple2._1()+":"+tuple2._2());
                return tuple2;
            }
        });
        //System.out.println(mapToPairResult.collect());

        //reduceByKey 统计词频
        JavaPairRDD reduceByKeyResult = mapToPairResult.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer i1, Integer i2) throws Exception {
                return i1+i2;
            }
        });
        //System.out.println(reduceByKeyResult.collect());

        //groupByKey 统计词频
        JavaPairRDD groupByKeyResult = mapToPairResult.groupByKey();
        //System.out.println(groupByKeyResult.collect());
        //System.out.println(mapToPairResult.collectAsMap());


        /**
         * flatMap 用于切分单词 Arrays.asList("hello world","world")-->hello,world,world
         * 与map不同的是：flatMap返回的是一个可迭代的List
         */
        JavaRDD flatMapResult = lines.flatMap(new FlatMapFunction() {
            @Override
            public Iterable call(Object line) throws Exception {
                return Arrays.asList(line.toString().split(" "));
            }
        });
        //System.out.println(flatMapResult.collect());

        /**
         * spark有两个共享变量 累加器Accumulator  和广播变量Broadcast
         * 累加器Accumulator：用于全局统计
         * Broadcast广播变量：高效分发大的变量或者小表，实现本地聚合
         */
         final Accumulator countSpace = jsc.accumulator(0);
         JavaRDD accumulatorResult = lines.flatMap(new FlatMapFunction() {
             @Override
             public Iterable call(Object o) throws Exception {
                 System.out.println(o.toString());
                 if(o.toString().equals("")){
                     countSpace.add(1);
                 }
                 return Arrays.asList(o.toString().split(" "));
             }
         });
         //如果在accumulatorResult.collect()之前打印countSpace，值是1，因为没有action算子，job还没有真正执行
         //累加器还没有累加
         //System.out.println("提交之前打印："+countSpace);
         //System.out.println(accumulatorResult.collect());
         //System.out.println("提交之后打印："+countSpace);

        //broadcast
        List<Integer> data = Arrays.asList(5, 1, 1, 4, 4, 2, 2);
        JavaRDD<Integer> javaRDD = jsc.parallelize(data,5);
        final Broadcast<List<Integer>> broadcast = jsc.broadcast(data);
        JavaRDD<Integer> result = javaRDD.map(new Function<Integer, Integer>() {
            List<Integer> iList = broadcast.value();
            @Override
            public Integer call(Integer v1) throws Exception {
                Integer isum = 0;
                for(Integer i : iList)
                    isum += i;
                return v1 + isum;
            }
        });
        System.out.println(result.collect());


    }
}
