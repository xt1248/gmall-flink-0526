package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;

public class MyDeserialization implements DebeziumDeserializationSchema<String> {

    /**
     *{
     *  "database":"",
     *  "tableName":"",
     *  "after":{"id":"1001","name":"zs"...},
     *  "before":{"id":"1001","name":"zs"...},
     *  "type":"insert"
     *}
     */
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        //1.创建JSONObject对象用来存放最终结果
        JSONObject result = new JSONObject();

        //TODO 获取数据库&表名
        String topic = sourceRecord.topic();
        String[] split = topic.split("\\.");
        String database = split[1];
        String tableName = split[2];

        //TODO 获取before&after数据
        Struct value = (Struct) sourceRecord.value();

        //TODO after
        Struct after = value.getStruct("after");
        JSONObject afterJSON = new JSONObject();
        //判断是否有after数据
        if (after!=null){
            Schema schema = after.schema();
            List<Field> fields = schema.fields();
            for (Field field : fields) {
                afterJSON.put(field.name(), after.get(field));
            }
        }

        //TODO before
        Struct before = value.getStruct("before");
        JSONObject beforeJSON = new JSONObject();
        //判断是否有after数据
        if (before!=null){
            Schema schema = before.schema();
            List<Field> fields = schema.fields();
            for (Field field : fields) {
                beforeJSON.put(field.name(), before.get(field));
            }
        }

        //TODO 获取操作类型 DELETE UPDATE CREATE
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        String type = operation.toString().toLowerCase();
        if ("create".equals(type)){
            type = "insert";
        }

        result.put("database", database);
        result.put("tableName", tableName);
        result.put("after", afterJSON);
        result.put("before", beforeJSON);
        result.put("type", type);

        collector.collect(result.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
