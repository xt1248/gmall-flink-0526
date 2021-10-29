package com.atguigu.app.ods;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.atguigu.app.func.MyDeserialization;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink_CDCWithCustomerSchema {

    public static void main(String[] args) throws Exception {

        //1.获取流的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //设置Mysql的相关属性
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("123456")
                .databaseList("gmall_flink_0526")
                .startupOptions(StartupOptions.latest())
                .deserializer(new MyDeserialization())
                .build();

        //将flinkCDC添加到source中
        DataStreamSource<String> streamSource = env.addSource(sourceFunction);

        //将数据发送至kafka
        streamSource.print();
        streamSource.addSink(MyKafkaUtil.getKafkaSink("ods_base_db"));

        env.execute();
    }

}
