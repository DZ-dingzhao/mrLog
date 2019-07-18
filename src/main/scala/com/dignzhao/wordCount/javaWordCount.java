package com.dignzhao.wordCount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class javaWordCount {

    public static void main(String[] args) {

        //获取JavaSparkContext
        SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("JavaSparkWordCount");
        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        //统计单词个数

        //读取文件
        JavaRDD<String> file = sparkContext.textFile("H:\\深圳大数据6期hadoop\\spark课程\\1、spark第一天\\wordcount");

        //切割数据
        JavaRDD<String> flatMap = file.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String file) throws Exception {

                String[] split = file.split(" ");
                Iterator<String> iterator = Arrays.asList(split).iterator();
                return iterator;
            }
        });

        //统计单词出现的个数
        JavaPairRDD<String, Integer> mapToPair = flatMap.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {

                Tuple2<String, Integer> tuple2 = new Tuple2<String, Integer>(word, 1);

                return tuple2;
            }
        });

        //将相同key的单词进行汇总
        JavaPairRDD<String, Integer> reduce = mapToPair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {

                return v1 + v2;
            }
        });


        reduce.saveAsTextFile("H:\\深圳大数据6期hadoop\\spark课程\\1、spark第一天\\wordcount\\out__wordCount");

        sparkContext.stop();

    }


}
