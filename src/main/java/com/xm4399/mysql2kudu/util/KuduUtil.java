package com.xm4399.mysql2kudu.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.val;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kudu.*;
import org.apache.kudu.client.*;
import scala.annotation.meta.field;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.text.FieldPosition;
import java.util.List;

public class KuduUtil implements Serializable {

    private static KuduClient kuduClient = new KuduClient.KuduClientBuilder("10.20.0.197:7051,10.20.0.198:7051,10.20.0.199:7051")
            .defaultAdminOperationTimeoutMs(60000).defaultSocketReadTimeoutMs(60000).defaultOperationTimeoutMs(60000).build();
    // 获取一个会话
    private static final KuduSession session = getKuduSession();


    private static ColumnTypeAttributes decimalCol = new ColumnTypeAttributes.ColumnTypeAttributesBuilder().precision(15).scale(4).build();


    public KuduClient getKuduClient(){
        if(kuduClient != null){
            return kuduClient;
        }else{
        KuduClient kuduClient = new KuduClient.KuduClientBuilder("10.20.0.197:7051,10.20.0.198:7051,10.20.0.199:7051")
                .defaultAdminOperationTimeoutMs(60000).defaultSocketReadTimeoutMs(60000).defaultOperationTimeoutMs(60000).build();

        return kuduClient;
        }
    }

    public static KuduSession getKuduSession(){

        final KuduSession session = kuduClient.newSession();
        session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
        //      session.setTimeoutMillis(AsyncKuduClient.DEFAULT_OPERATION_TIMEOUT_MS)
        session.setIgnoreAllDuplicateRows(true);
        session.setMutationBufferSpace(1000);
        return session;
    }

    public  void deleteRecordFromKudu(KuduTable kuduTable, ConsumerRecord<String,String> record) throws KuduException {
        this.delOneRow(kuduTable,record);
    }

    private void delOneRow(KuduTable kuduTable, ConsumerRecord<String,String> record) throws KuduException {
        JSONArray array = JSON.parseArray(JSON.parseObject(record.value()).get("data").toString());
        //String tableName =
        int len = array.size();
        if(len > 0){
            for (int i = 0; i < len; i++) {
                KuduSession session = getKuduSession();
                Delete delete = kuduTable.newDelete();
                PartialRow row = delete.getRow();

                JSONObject data = array.getJSONObject(i);
                String pk = "";  //kudu主键对应的内容
                Schema colSchema = kuduTable.getSchema();
                List <ColumnSchema> pkList = colSchema.getPrimaryKeyColumns();

                for(ColumnSchema item : pkList){
                    String colName = item.getName();
                    if("table_name" .equals(colName)){
                        String subTableName = JSON.parseObject(record.value()).get("table").toString();
                        pk = subTableName;  //kudu主键对应的内容
                    }else {
                        pk = data.getOrDefault(colName,"").toString();
                    }
                    //int colIdx = schema.getColumnIndex(name);
                    int colIdx = colSchema.getColumnIndex(colName);
                    Type colType = item.getType();
                    Common.DataType dataType = colType.getDataType(decimalCol);
                    if(!"".equals(pk)){
                        addRow(row,pk,colName,colIdx,colType,dataType);
                    }
                }
                session.apply(delete);
            }
        }

    }

    public void upsertRecordToKudu(KuduTable kuduTable, ConsumerRecord<String,String> record) throws KuduException {
        System.out.println("进入upsertRecordToKudu方法>>>>>>>>>>>>>>>>>>>>>>>");
        JSONArray array = JSON.parseArray(JSON.parseObject(record.value()).get("data").toString());
        int len = array.size();
        System.out.println("dataArrayd的长度为>>>>>>>>>>>>>>>>>>>>" +len);
        if(len > 0){
            for (int i = 0; i < len ; i++) {
                System.out.println("此时的索引为>>>>>>>>>>>>>>" + i);
                Upsert upsert = kuduTable.newUpsert();
                PartialRow row = upsert.getRow();
                JSONObject fieldValue = array.getJSONObject(i);
                String fieldValueStirng = fieldValue.toString();
                System.out.println("字段的内容为>>>>>>>>>>>>>"  + fieldValueStirng);
                Schema colSchema =kuduTable.getSchema();
                List<ColumnSchema> colList = colSchema.getColumns();
                System.out.println("获取到columnList>>>>>>>>>>>>>>>>>>>>");
                for(ColumnSchema item : colList){
                    System.out.println("进入colList的循环:>>>>>>>>");
                    String colName = item.getName();
                    System.out.println("kudu表的字段名>>>>>>>>>>" + colName);
                    //int colIdx = this.getColumnIndex(colSchema,colName);
                    int colIdx = colSchema.getColumnIndex(colName);
                    Type colType = item.getType();
                    Common.DataType dataType = colType.getDataType(decimalCol);
                    System.out.println("获取到dataType>>>>>>>>>>>>>>>>>" + dataType.toString());
                    if(fieldValue.containsKey(colName)){
                        System.out.println("fieldValue包含了kudu字段名>>>>>>>>");
                        try{
                           String field = fieldValue.get(colName).toString();
                            System.out.println("要加入的字段内容为>>>>>>>>>>" + field);
                           //kudu单元格最大不超过64k,当内容超过16384位,将其截断
                            if(field.length() >= 16384){
                                System.out.println("有超过16384字符>>>>>>>>");
                                field = field.substring(0,16380);
                            }
                            addRow(row,field,colName,colIdx,colType,dataType);
                            System.out.println("有错吗？");
                        }catch (Exception e){
                            e.printStackTrace();
                        }
                    }else if("table_name" .equals(colName)){
                        String subTableName = JSON.parseObject(record.value()).getOrDefault("table","").toString();
                        addRow(row,subTableName,colName,colIdx,colType,dataType);
                    }
                }
            session.apply(upsert);
            session.flush();

            }
        }
    }
    private int getColumnIndex(Schema columns, String colName){
        try{
            System.out.println("获取kudu字段名在schema的索引>>>>>>>>>>");
            System.out.println(columns.getColumnIndex(colName));
            return columns.getColumnIndex(colName);
            //System.out.println(columns.getColumnIndex(colName));
        }catch(Exception e){
            e.printStackTrace();
        }finally {

            return -1;
        }
    }

    public KuduTable getKuduTable(String tableName) throws KuduException {
        KuduTable kuduTable = null;
        if (kuduClient == null){
            kuduClient = getKuduClient();
        }
        kuduTable = kuduClient.openTable(tableName);
        return kuduTable;
    }

    private void addRow(PartialRow row, String field, String colName, int colIdx, Type colType, Common.DataType dataType ){
        switch(dataType){
            case BOOL :
                row.addBoolean(colIdx, Boolean.parseBoolean(field));
                break;
            case FLOAT :
                row.addFloat(colIdx, Float.parseFloat(field));
                break;
            case DOUBLE :
                row.addDouble(colIdx, Double.parseDouble(field));
                break;
            case BINARY :
                row.addBinary(colIdx, field.getBytes());
                break;
            case INT8 :
                row.addByte(colIdx, Byte.parseByte(field));
                break;
            case INT16 :
                //                            val temp = row.getShort(colName).toShort
                row.addShort(colIdx, Short.parseShort(field));
                break;
            case INT32 :
                row.addInt(colIdx, Integer.parseInt(field));
                break;
            case INT64 :
                row.addLong(colIdx, Long.parseLong(field));
                break;
            case STRING :
                row.addString(colIdx, field) ;
                break;
            case DECIMAL64 :
                row.addDecimal(colIdx,new BigDecimal(field,new MathContext(15,RoundingMode.HALF_UP)).setScale(4,RoundingMode.HALF_UP));

                break;
            default:
                throw new IllegalArgumentException("The provided data type doesn't map to know any known one.");
        }
    }


    public void close() throws KuduException {
        if (session != null){
            session.close();
        }
        if (kuduClient != null){
            kuduClient.close();
        }
    }




}
