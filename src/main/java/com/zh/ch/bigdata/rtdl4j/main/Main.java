package com.zh.ch.bigdata.rtdl4j.main;

import com.zh.ch.bigdata.rtdl4j.flink.connectors.kafka.RealTimeKafkaConsumer;
import com.zh.ch.bigdata.rtdl4j.util.exception.ProjectException;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

public class Main {


    public static void main(String[] args) throws ProjectException, IOException {

        String kafkaConfigPropertiesFilePath = "";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dataStream = env.addSource(RealTimeKafkaConsumer.get(kafkaConfigPropertiesFilePath));




    }


}
