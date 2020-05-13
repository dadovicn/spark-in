package com.wjl.count.keyword;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;
import java.util.regex.Pattern;

@Component
public class WordCountService implements Serializable, CommandLineRunner {
    private static final Pattern SPACE = Pattern.compile(" ");

    @Autowired
    private transient JavaSparkContext sc;

    @Override
    public void run(String... args) {
        Map<String, Integer> result = new HashMap<>();
        JavaRDD<String> lines = sc.textFile("/Users/duandongdong/Downloads/宇宙巨校闪级生.txt").cache();

        lines.map((a) -> a);

        System.out.println(lines.count());

        //将文本数据按行处理，每行按空格拆成一个数组,flatMap会将各个数组中元素合成一个大的集合
        //这里需要注意的是FlatMapFunction中<String, String>,第一个表示输入，第二个表示输出
        //与Hadoop中的map-reduce非常相似
        JavaRDD<String> splitRDD = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        //处理合并后的集合中的元素，每个元素的值为1，返回一个Tuple2,Tuple2表示两个元素的元组
        //值得注意的是上面是JavaRDD，这里是JavaPairRDD，在返回的是元组时需要注意这个区别
        //PairFunction中<String, String, Integer>，第一个String是输入值类型
        //第二第三个，String, Integer是返回值类型
        //这里返回的是一个word和一个数值1，表示这个单词出现一次
        JavaPairRDD<String, Integer> splitFlagRDD = splitRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s,1);
            }
        });


        //reduceByKey会将splitFlagRDD中的key相同的放在一起处理
        //传入的（x,y）中，x是上一次统计后的value，y是本次单词中的value，即每一次是x+1
        JavaPairRDD<String, Integer> countRDD = splitFlagRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer+integer2;
            }
        });

        //将计算后的结果存在项目目录下的result目录中
        countRDD.saveAsTextFile("./resultJava");

        System.out.println(result);
    }
}