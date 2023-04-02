package com.e2mg.bigdata.flink.sink;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

/**
 * kafka测试
 *
 * @author EdiwalMusk
 * @date 2023/4/2 11:47
 */
public class KafkaToESStream {

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

        List<HttpHost> hosts = new ArrayList<>();
        hosts.add(new HttpHost("192.168.94.194", 9200));
        hosts.add(new HttpHost("192.168.94.195", 9200));
        hosts.add(new HttpHost("192.168.94.196", 9200));
        ElasticsearchSink.Builder<String> builder = new ElasticsearchSink.Builder<>(hosts, new ElasticsearchSinkFunction<String>() {
            @Override
            public void process(String s, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                HashMap<String, String> map = new HashMap<>();
                map.put("value", s);
                IndexRequest indexRequest;
                try {
                    indexRequest = Requests.indexRequest()
                            .index("flink_index")
                            .type("_doc")
                            .source(new ObjectMapper().writeValueAsString(map), XContentType.JSON);
                    requestIndexer.add(indexRequest);
                } catch (JsonProcessingException e) {
                    e.printStackTrace();
                }
            }

            @Override
            public void open() throws Exception {
            }

            @Override
            public void close() throws Exception {

            }
        });
        builder.setBulkFlushMaxActions(1);

        kafkaStream.addSink(builder.build());

        env.execute();
    }
}
