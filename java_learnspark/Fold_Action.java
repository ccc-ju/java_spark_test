package java_learnspark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by ycz on 2018/3/20.
 * fold 类似于aggregate，但是函数只有一个，也是分区内先和初始值合并，在整体合并
 */
public class Fold_Action {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("Fold_Action");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        List<String> data = Arrays.asList("5", "1", "1", "3", "6", "2", "2");
        JavaRDD<String> javaRDD = jsc.parallelize(data,5);
        JavaRDD<String> partitionRDD = javaRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer v1, Iterator<String> v2) throws Exception {
                LinkedList<String> linkedList = new LinkedList<String>();
                while(v2.hasNext()){
                    linkedList.add(v1 + "=" + v2.next());
                }
                return linkedList.iterator();
            }
        },false);

        System.out.println(partitionRDD.collect());

        String foldRDD = javaRDD.fold("1", new Function2<String, String, String>() {
            @Override
            public String call(String v1, String v2) throws Exception {
                return (Integer.parseInt(v1)+Integer.parseInt(v2))+"";
            }
        });
        //[0=5, 1=1, 2=1, 2=3, 3=6, 4=2, 4=2]
        //0 :0 :5 :0 :1 :0 :1 :3 :0 :6 :0 :2 :2
        System.out.println("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~" + foldRDD);//26

    }
}
