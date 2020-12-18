package com.zhaofanqi.dataSet;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Author: zhaofanqi
 * @TIME: Created in 14:00 2020/12/18
 * @Desc:  自定义实现 FlatMap
 */

public class MyFlatMapFunction implements FlatMapFunction<String, Tuple2<String,Integer>> {
    //  flatMap  其中 s 是输入   Collector是收集器，用于将处理结果返回
    public void flatMap(String s, Collector<Tuple2<String,Integer>> collector) throws Exception {
        // 得到词组
        String[] words = s.split(" ");//得到输入的词组，这个是按照行分割的
        // 构建 tuple2(word ,1)
        for(String word:words ){
            // 将结果存入收集器
            collector.collect(new Tuple2<String,Integer>(word,1));
        }
    }
}
