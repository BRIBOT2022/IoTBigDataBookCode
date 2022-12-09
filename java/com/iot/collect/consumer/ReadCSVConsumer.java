package com.iot.collect.consumer;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import java.util.Properties;

public class ReadCSVConsumer {

    public static void main(String[] args) throws Exception {
        //获取环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行数为1
        env.setParallelism(1);

        //准备kafka消费者配置信息
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "node01:9092");
        properties.put("group.id", "csvtoflink");

        //创建kafka消费者（SimpleStringSchema对象中已提供kv反序列化模板）
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>("first",
                                                                            new SimpleStringSchema(),
                                                                            properties);
        //通过kafkaConsumer从kafka中读取数据到flink
        DataStream<String> myStream = env.addSource(kafkaConsumer);

        myStream.map(new MapFunction<String, String>() {
            private static final long serialVersionUID = -6867736771747690202L;

            @Override
            public String map(String value) throws Exception {
                return "Stream Value: " + value;
            }
        }).print();

        //执行程序
        env.execute();
    }

}