package com.xm4399.mysql2kudu.test;

import com.xm4399.mysql2kudu.util.KafkaStringSchema;
import com.xm4399.mysql2kudu.util.KuduSink;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Properties;

public class KafkaConsumer4Canal {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //  从kafka中读取数据
        // 创建kafka相关的配置
        Properties properties = new Properties();
        //properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("bootstrap.servers", "10.0.0.194:9092,10.0.0.195:9092,10.0.0.199:9092");
        properties.setProperty("group.id", "aa");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
        Properties props = new Properties();

        DataStreamSink<ConsumerRecord<String,String>> stream = env
                .addSource(new FlinkKafkaConsumer<ConsumerRecord<String,String>>("chenzhikun_test", new KafkaStringSchema(), properties))
                .addSink(new KuduSink());
        env.execute();
    }

}
