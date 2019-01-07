package spark; /**
 * @ClassName ss.d
 * @Description TODO
 * @Author pppLLL
 * @Date 2018/11/14 17:38
 * @Version 1.0
 **/


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * @ClassName SparkWordCount_java
 * @Author lyd
 * @Date $ $
 * @Vesion 1.0
 **/
public class java版sparkWordcount{
    public static void main(String[] args) {
        //1、获取sparkConf
        SparkConf conf = new SparkConf().setAppName("spark_wordcount_java").setMaster("local");
        //2、通过conf获取JavaSparkContext
        JavaSparkContext jsc = new JavaSparkContext(conf);

        //通过jsc来进行操作
        JavaRDD<String> lines = jsc.textFile(args[0]);
        //拆分
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });
        //映射
        JavaPairRDD<String,Integer> word_one = words.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s,1);
            }
        });
        //聚合
        JavaPairRDD<String,Integer> reduce_word_count =  word_one.reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        //输出
        reduce_word_count.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println(t._1+":"+t._2);
            }
        });

        //关闭jsc
        jsc.stop();
        /**
         * 该可用
         * 提交自己的jar到spark集群
         * rdd和提交流程
         */
    }
}
