package com.xm4399.mysql2kudu.test;

import com.xm4399.mysql2kudu.util.KafkaStringSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumer4Canal {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //  从kafka中读取数据
        // 创建kafka相关的配置
        Properties properties = new Properties();
       // properties.setProperty("bootstrap.servers", "10.0.0.194:9092,10.0.0.195:9092,10.0.0.199:9092");
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "canal_chenzhikun_test_group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
        Properties props = new Properties();
        FlinkKafkaConsumer<ConsumerRecord<String,String>> kafkaSource = new FlinkKafkaConsumer<ConsumerRecord<String, String>>("first_canal", new KafkaStringSchema(), props);
        env.addSource(kafkaSource).print();


        env.execute();



    }
    class Mysql2Kudu extends RichSinkFunction<ConsumerRecord<String,String>>{
       // final String  kuduMaseter =null ;


    }
}