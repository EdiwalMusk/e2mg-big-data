package com.e2mg.bigdata.flink;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.stream.Stream;

/**
 * 描述
 *
 * @author EdiwalMusk
 * @date 2023/4/2 11:47
 */
public class SocketWordCount {

    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.从文件读取数据
        DataStreamSource<String> lineDataSource = env.socketTextStream("192.168.94.194", 7777);

        // 3.转换二元组
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOneTuple =
                lineDataSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
                    String[] words = line.split(" ");
                    Stream.of(words).forEach(word -> {
                        out.collect(Tuple2.of(word, 1L));
                    });

                }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        // 4.分组
        KeyedStream<Tuple2<String, Long>, String> keyedStream = wordAndOneTuple.keyBy(data -> data.f0);

        // 5.聚合
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = keyedStream.sum(1);

        // 6.打印结果
        sum.print();

        // 7.执行
        env.execute();
    }
}
