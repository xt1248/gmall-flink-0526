package com.atguigu.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

//数据流：web/app -> nginx -> 日志服务器 -> Kafka(ODS) -> FlinkApp    -> Kafka(DWD)
//程  序：MockLog -> nginx -> Logger    -> Kafka(ZK)  -> BaseLogApp  -> Kafka
public class BaseLogApp {

    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);   //生产环境应该与Kafka的分区数保持一致

        //TODO 2.读取 Kafka ods_base_log 主题的数据创建流
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaSource("ods_base_log", "base_log_app_210526"));

        //TODO 3.将数据转换为 JSON 对象(注意提取脏数据)
        OutputTag<String> dirtyTag = new OutputTag<String>("Dirty") {
        };
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {

                try {
                    JSONObject jsonObject = JSON.parseObject(value);

                    //将转换成功的 数据写入主流
                    out.collect(jsonObject);

                } catch (Exception e) {
                    //转换失败,将数据写入侧输出流
                    ctx.output(dirtyTag, value);
                }
            }
        });

        //打印测试
//        jsonObjDS.print("JSON>>>>>>>>>>>");
        jsonObjDS.getSideOutput(dirtyTag).print("Dirty>>>>>>>>>>");

        //TODO 4.按照 mid 分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        //TODO 5.新老用户校验  状态编程
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {

            private ValueState<String> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("value-state", String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {

                //获取新老用户标记
                String isNew = value.getJSONObject("common").getString("is_new");

                //判断是否为"1"
                if ("1".equals(isNew)) {

                    //获取状态数据,并判断状态是否为null
                    String state = valueState.value();

                    if (state == null) {
                        //更新状态
                        valueState.update("1");
                    } else {
                        //更新数据
                        value.getJSONObject("common").put("is_new", "0");
                    }
                }

                //返回结果
                return value;
            }
        });

//        jsonObjWithNewFlagDS.print("JSON>>>>>>>>>>>");

        //TODO 6.分流  将页面日志  主流   启动和曝光  侧输出流
        OutputTag<JSONObject> startTag = new OutputTag<JSONObject>("start") {
        };
        OutputTag<JSONObject> displayTag = new OutputTag<JSONObject>("display") {
        };
        SingleOutputStreamOperator<JSONObject> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {

                //获取启动数据
                String start = value.getString("start");
                if (start != null) {

                    //将数据写入启动日志侧输出流
                    ctx.output(startTag, value);

                } else {

                    //将数据写入页面日志主流
                    out.collect(value);

                    //获取曝光数据字段
                    JSONArray displays = value.getJSONArray("displays");

                    //判断是否存在曝光数据
                    Long ts = value.getLong("ts");
                    String pageId = value.getJSONObject("page").getString("page_id");
                    if (displays != null && displays.size() > 0) {

                        //遍历曝光数据,写出数据到曝光侧输出流
                        for (int i = 0; i < displays.size(); i++) {

                            JSONObject display = displays.getJSONObject(i);

                            //补充页面id字段和时间戳
                            display.put("ts", ts);
                            display.put("page_id", pageId);

                            //将数据到曝光侧输出流
                            ctx.output(displayTag, display);
                        }
                    }
                }
            }
        });

        //TODO 7.提取各个流的数据
        DataStream<JSONObject> startDS = pageDS.getSideOutput(startTag);
        DataStream<JSONObject> displayDS = pageDS.getSideOutput(displayTag);

        pageDS.print("Page>>>>>>>>>>>");
        startDS.print("Start>>>>>>>>>>>");
        displayDS.print("Display>>>>>>>>>");

        //TODO 8.将数据写入 Kafka 主题
        pageDS.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getKafkaSink("dwd_page_log"));
        startDS.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getKafkaSink("dwd_start_log"));
        displayDS.map(JSONAware::toJSONString).addSink(MyKafkaUtil.getKafkaSink("dwd_display_log"));

        //TODO 9.启动任务
        env.execute("BaseLogApp");

    }

}
