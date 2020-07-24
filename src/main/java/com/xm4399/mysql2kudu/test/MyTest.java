package com.xm4399.mysql2kudu.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.util.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class MyTest {

    public static void checkQQ(){
        String qq = "1_2__gg_33";
        String regex = "([\\s\\S]*)(_[0-9]{1,3}$)";
        if(qq.matches(regex)){
            System.out.println(qq.substring(0,qq.lastIndexOf("_")));
        }
        else{
            System.out.println(qq+"... 不合法");}

    }

    public static void main(String[] args) {
        //checkQQ();
        //System.out.println(StringUtils.isNullOrWhitespaceOnly(null));
        /*ConsumerRecord<String,String> json = "{\"data\":[{\"id\":\"1\",\"age\":\"1\",\"username\":\"a\"}]," +
                "\"database\":\"chenzhikun_test\",\"es\":1595468026000,\"id\":85675,\"isDdl\":false,\"mysqlType\":{\"id\":\"int(11)\",\"age\":\"int(11)\",\"username\":\"varchar(50)\"},\"old\":null,\"pkNames\":[\"id\"],\"sql\":\"\",\"" +
                "sqlType\":{\"id\":4,\"age\":4,\"username\":12},\"table\":\"chenzhikun_increase\",\"ts\":1595468026445,\"type\":\"INSERT\"}";
        jsonArrTest(json);*/
    }

    public  static void jsonArrTest(ConsumerRecord<String,String> record){
        JSONArray arr = JSON.parseArray(JSON.parseObject(record.value()).get("data").toString());
        for(int i =0; i < arr.size(); i++){
            JSONObject fieldValue = arr.getJSONObject(i);
            System.out.println(fieldValue.getString("id"));

        }

    }

}
