package com.tutorial.demo;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class consumerDemo {
    public static void main(String[] args) {
        String topic = "topic_demo";
        System.out.println("Hello World");
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "My fourth App");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create consumer
        KafkaConsumer<String, String> consumer =  new KafkaConsumer<String, String>(properties);

        //subscribe consumer
        consumer.subscribe(Arrays.asList(topic));

        //poll for new data
        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> oneRec : consumerRecords) {
                System.out.println("Key: " + oneRec.key() + "value : " + oneRec.value() +
                        "Partition: " + oneRec.partition() + "offset : " + oneRec.offset());

            }
        }
    }
}
