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
import org.springframework.util.StringUtils;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;

/**
 * spark 统计 demo
 */
@Component
public class WordCountService implements Serializable, CommandLineRunner {

    @Autowired
    private transient JavaSparkContext sc;

    @Override
    public void run(String... args) {
        // 文件可以是本地文件系统的文件, 也可以是 hdfs 文件系统的文件
        JavaRDD<String> lines = sc.textFile("/Users/duandongdong/Downloads/测试文件.txt").cache();
        countStr("关键词", lines);
        wordFrequency(lines);
    }

    /**
     * 统计关键词出现次数
     * @param keyWord 关键词
     * @param lines 行 RDD
     */
    public void countStr(String keyWord, JavaRDD<String> lines) {
        lines.map((a) -> a);
        JavaRDD<Integer> spu = lines.map(i -> StringUtils.countOccurrencesOf(i.toString(), keyWord));
        int c = spu.reduce((a, b) -> a + b);
    }

    /**
     * 统计词频, 输出到文件
     * @param lines 行 RDD
     */
    public void wordFrequency(JavaRDD<String> lines) {

        // 空格拆分, 单词集合
        JavaRDD<String> splitRDD = lines.flatMap((FlatMapFunction<String, String>) s ->
                Arrays.asList(s.split(" ")).iterator());

        // 处理合并后的集合中的元素，每个元素的值为1，返回一个Tuple2,Tuple2表示两个元素的元组
        // PairFunction中<String, String, Integer>，第一个String是输入值类型, String, Integer是返回值类型
        // 这里返回的是一个word和一个数值1，表示这个单词出现一次
        JavaPairRDD<String, Integer> splitFlagRDD = splitRDD.mapToPair((PairFunction<String, String, Integer>) s ->
                new Tuple2<>(s,1));

        // 归约处理
        //传入的（x,y）中，x是上一次统计后的value，y是本次单词中的value，即每一次是x+1
        JavaPairRDD<String, Integer> countRDD = splitFlagRDD.reduceByKey((Function2<Integer, Integer, Integer>)
                (integer, integer2) -> integer+integer2);

        //将计算后的结果存在项目目录下的result目录中
        countRDD.saveAsTextFile("./result");
    }
}