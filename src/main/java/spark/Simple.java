package spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.util.Arrays;
import java.util.List;

/**
 * @ClassName Simple
 * @Description TODO
 * @Author PL
 * @Date 2018/11/15 10:55
 * @Version 1.0
 * @return 666666666666
 **/

public class Simple {
    public static void main(String[] args) {
        SparkConf conf=new SparkConf().setAppName("pl").setMaster("local");
        JavaSparkContext js=new JavaSparkContext(conf);
        List<Integer> data= Arrays.asList(1,2,3,4,5,6);
       JavaRDD<Integer> rdd=js.parallelize(data); 
       
    }
    public static void test_map(JavaRDD<Integer>rdd){
       rdd.map(new Function<Integer, Object>() {
           public Object call(Integer v1) throws Exception {
               return  v1*5;
           }
       });

    }
}
