package com.e2mg.bigdata.flink.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

/**
 * kafka测试
 *
 * @author EdiwalMusk
 * @date 2023/4/2 11:47
 */
public class KafkaToRedisStream {

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

        // 集群配置
        FlinkJedisConfigBase config = getClusterConfig();

        // 单点配置
        // FlinkJedisConfigBase config = getPoolConfig();

        kafkaStream.addSink(new RedisSink<>(config, new MyRedisMapper()));

        env.execute();
    }

    private static FlinkJedisPoolConfig getPoolConfig() {
        return new FlinkJedisPoolConfig.Builder().setHost("192.168.94.194").setPort(7379).build();
    }

    private static FlinkJedisClusterConfig getClusterConfig() {
        Set<InetSocketAddress> isas = new HashSet<>();
        InetSocketAddress isa;
        isa = new InetSocketAddress("192.168.94.194", 6379);
        isas.add(isa);
        isa = new InetSocketAddress("192.168.94.194", 6380);
        isas.add(isa);
        isa = new InetSocketAddress("192.168.94.194", 6381);
        isas.add(isa);
        isa = new InetSocketAddress("192.168.94.194", 6382);
        isas.add(isa);
        isa = new InetSocketAddress("192.168.94.194", 6383);
        isas.add(isa);
        isa = new InetSocketAddress("192.168.94.194", 6384);
        isas.add(isa);

        FlinkJedisClusterConfig config = new FlinkJedisClusterConfig.Builder().setNodes(isas).build();

        return config;
    }

    public static class MyRedisMapper implements RedisMapper<String> {
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "clicks");
        }

        @Override
        public String getKeyFromData(String s) {
            return s;
        }

        @Override
        public String getValueFromData(String s) {
            return s;
        }
    }
}
