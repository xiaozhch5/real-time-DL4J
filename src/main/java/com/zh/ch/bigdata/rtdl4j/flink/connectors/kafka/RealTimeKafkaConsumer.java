package com.zh.ch.bigdata.rtdl4j.flink.connectors.kafka;

import com.zh.ch.bigdata.rtdl4j.flink.constants.KafkaConfigConstant;
import com.zh.ch.bigdata.rtdl4j.util.exception.ProjectException;
import com.zh.ch.bigdata.rtdl4j.util.properties.PropertiesAnalyzeUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;

public class RealTimeKafkaConsumer {
    public static FlinkKafkaConsumer<String> get(String kafkaConfigPropertiesFilePath) throws ProjectException, IOException {
        Properties properties = new Properties();
        properties.setProperty(
                KafkaConfigConstant.BOOTSTRAP_SERVER,
                PropertiesAnalyzeUtil.getProperty(kafkaConfigPropertiesFilePath, KafkaConfigConstant.BOOTSTRAP_SERVER));
        properties.setProperty(
                KafkaConfigConstant.GROUP_ID,
                PropertiesAnalyzeUtil.getProperty(kafkaConfigPropertiesFilePath, KafkaConfigConstant.GROUP_ID));
        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<>(
                PropertiesAnalyzeUtil.getProperty(kafkaConfigPropertiesFilePath, KafkaConfigConstant.BOOTSTRAP_SERVER),
                new SimpleStringSchema(),
                properties);
        flinkKafkaConsumer.setStartFromEarliest();
        flinkKafkaConsumer.setStartFromGroupOffsets();
        flinkKafkaConsumer.assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(20)));
        return flinkKafkaConsumer;
    }
}
