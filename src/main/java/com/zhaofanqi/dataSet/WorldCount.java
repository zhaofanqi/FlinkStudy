package com.zhaofanqi.dataSet;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @Author: zhaofanqi
 * @TIME: Created in 13:43 2020/12/18
 * @Desc: 批处理 worldCount
 *
 * DataSet 处理的是离线数据
 */

public class WorldCount {
    public static void main(String[] args) throws Exception {
        // 创建执行文件
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 确定文件位置
        String inputFilePath = "./src/main/resources/hello.txt";
        // 读取数据 DataSet 是 底层抽象类
        DataSource<String> dataSource = env.readTextFile(inputFilePath);
        // 对数据进行切分
        // java版本的 flatMap需要自己去实现
        DataSet<Tuple2<String, Integer>> resultSet = dataSource.flatMap(new MyFlatMapFunction())
                .groupBy(0)// 按照第一个位置进行分词
                .sum(1);// 按照第二个位置上的数据进行求和
//        System.out.println(resultSet);  显示的是一个对象
        resultSet.print();
    }
}
