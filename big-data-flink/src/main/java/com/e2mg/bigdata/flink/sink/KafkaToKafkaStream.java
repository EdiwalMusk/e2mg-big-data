package com.e2mg.bigdata.flink.sink;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * kafka测试
 *
 * @author EdiwalMusk
 * @date 2023/4/2 11:47
 */
public class KafkaToKafkaStream {

    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.94.194:9092");
        properties.setProperty("group.id", "flink_group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        DataStreamSource<String> kafkaStream = env.addSource(new FlinkKafkaConsumer<>("clicks",
                new SimpleStringSchema(), properties));

        FlinkKafkaProducer<String> flinkKafkaProducer = new FlinkKafkaProducer<String>("192.168.94.194:9092",
                "flink_output", new SimpleStringSchema());
        kafkaStream.addSink(flinkKafkaProducer);

        env.execute();
    }
}
