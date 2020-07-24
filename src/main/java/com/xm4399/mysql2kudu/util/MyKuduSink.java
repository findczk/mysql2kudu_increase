package com.xm4399.mysql2kudu.util;

import com.alibaba.fastjson.JSON;
import org.apache.avro.data.Json;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class MyKuduSink  extends RichSinkFunction<ConsumerRecord<String,String>> {

    KuduClient kuduClient = null;
    KuduSession kuduSession = null;
    @Override
    public void invoke(ConsumerRecord<String, String> value, Context context) throws Exception {
        if( null == kuduClient || null == kuduSession ){
            kuduClient = new KuduClient.KuduClientBuilder("10.20.0.197:7051,10.20.0.198:7051,10.20.0.199:7051")
                    .defaultAdminOperationTimeoutMs(60000).defaultSocketReadTimeoutMs(60000).defaultOperationTimeoutMs(60000).build();
            // 获取一个会话
            KuduSession session = kuduClient.newSession();
            session.setTimeoutMillis(60000);
            session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
            session.setMutationBufferSpace(10000);
        }
        processEveryRow(value,kuduClient,kuduSession);


    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        kuduClient = new KuduClient.KuduClientBuilder("10.20.0.197:7051,10.20.0.198:7051,10.20.0.199:7051")
                .defaultAdminOperationTimeoutMs(60000).defaultSocketReadTimeoutMs(60000).defaultOperationTimeoutMs(60000).build();
        // 获取一个会话
        KuduSession session = kuduClient.newSession();
        session.setTimeoutMillis(60000);
        session.setFlushMode(SessionConfiguration.FlushMode.MANUAL_FLUSH);
        session.setMutationBufferSpace(10000);
    }

    @Override
    public void close() throws Exception {
        super.close();
        kuduClient.close();
        kuduSession.close();
    }

    //对每条数据进行处理
    public void processEveryRow(ConsumerRecord<String,String> row , KuduClient kuduClient, KuduSession kuduSession) throws KuduException {
        KuduUtil kuduUtil = new KuduUtil();
        String tableName = getTableName(row);
        KuduTable kuduTable = kuduClient.openTable(tableName);

        String[] tableNameArr = new String[]{};
        if(Arrays.asList(tableNameArr).contains(tableName)){
            String data = JSON.parseObject(row.value()).getOrDefault("data","").toString();
            if(StringUtils.isNullOrWhitespaceOnly(data)){
                System.out.println(tableName + "'s data is null.");
            }else{
                String rowType = JSON.parseObject(row.value()).getOrDefault("type","").toString();
                if("INSERT".equals(rowType) || "UPDATE".equals(rowType)){
                    kuduUtil.upsertRecordToKudu(kuduTable,row);
                }else if("DELETE".equals(rowType)){

                }

            }

        }else{
            System.out.println("表" + tableName + "不在kudu过滤范围");
        }

    }

    //获取表名
    public String getTableName(ConsumerRecord<String,String> row){
        String dbName = JSON.parseObject(row.value()).getOrDefault("database","").toString();
        String tableName = JSON.parseObject(row.value()).getOrDefault("table","").toString();
        String regex = "([\\s\\S]*)(_[0-9]{1,3}$)";
        if(tableName.matches(regex)){
            return  tableName.substring(0,tableName.lastIndexOf("_"));
        }
        else{
            return  tableName;
        }


    }




}
