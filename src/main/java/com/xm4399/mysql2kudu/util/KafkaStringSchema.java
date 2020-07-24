package com.xm4399.mysql2kudu.util;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.record.TimestampType;
import sun.awt.image.ShortInterleavedRaster;

import java.lang.reflect.Type;
import java.util.ArrayList;

public class KafkaStringSchema implements KafkaDeserializationSchema<ConsumerRecord<String, String>> {



    private ConsumerRecord<String,String> ConsumerRecord;
    private Object String;
    //private Object String;

    @Override
    public boolean isEndOfStream(ConsumerRecord<String, String> stringStringConsumerRecord) {
        return false;
    }

    @Override
    public ConsumerRecord<String, String> deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
        String key = null;
        if(consumerRecord.key() != null){
            key = new String(consumerRecord.key(),"UTF-8");
        }

        return new ConsumerRecord<String,String>(consumerRecord.topic(),
                consumerRecord.partition(),
                consumerRecord.offset(),
                consumerRecord.timestamp(),
                TimestampType.CREATE_TIME,
                consumerRecord.checksum(),
                consumerRecord.serializedKeySize(),
                consumerRecord.serializedValueSize(),
                key,
                new String(consumerRecord.value(),"UTF-8")
        );

    }

    @Override
    public TypeInformation<ConsumerRecord<String, String>> getProducedType() {
        /*ArrayList<String>.getclass;
        String.getClass();
        String.getClass();
        ConsumerRecord<String, String> ff=new ConsumerRecord<String,String>();
        ff.getClass();
        return TypeInformation.of();*/

        return TypeInformation.of(new TypeHint<ConsumerRecord<String,String>>(){});


    }
    //override def getProducedType: TypeInformation[ConsumerRecord[String, String]] = TypeInformation.of(classOf[ConsumerRecord[String, String]])

}
