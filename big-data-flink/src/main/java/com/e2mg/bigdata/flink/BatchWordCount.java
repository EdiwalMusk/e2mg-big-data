package com.e2mg.bigdata.flink;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.stream.Stream;

/**
 * 描述
 *
 * @author EdiwalMusk
 * @date 2023/4/2 11:47
 */
public class BatchWordCount {

    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2.从文件读取数据
        DataSource<String> lineDataSource = env.readTextFile("/home/workdir/flink/wordcount.txt");

        // 3.转换二元组
        FlatMapOperator<String, Tuple2<String, Long>> wordAndOneTuple =
                lineDataSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
                    String[] words = line.split(" ");
                    Stream.of(words).forEach(word -> {
                        out.collect(Tuple2.of(word, 1L));
                    });

                }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 4.分组
        UnsortedGrouping<Tuple2<String, Long>> wordAndOneGroup = wordAndOneTuple.groupBy(0);

        // 5.聚合
        AggregateOperator<Tuple2<String, Long>> sum = wordAndOneGroup.sum(1);

        // 6.打印结果
        // detached结果不能用输出，暂时输出到文件
        // sum.print();
        sum.writeAsCsv("/home/workdir/flink/wordcount_result.txt");

        env.execute();
    }
}
