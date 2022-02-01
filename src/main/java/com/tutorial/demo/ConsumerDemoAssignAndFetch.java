package com.tutorial.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoAssignAndFetch {
    public static void main(String[] args) {
        String topic = "topic_demo";
        System.out.println("Hello World");
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
       //Assign and seek are used to replay data or fetch a specific message
       TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        long offsetToReadFrom  =  15L;
        consumer.assign(Arrays.asList(partitionToReadFrom));
        consumer.seek(partitionToReadFrom, offsetToReadFrom);

        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessagesReadSoFar = 0;
        //poll for new data
        while (keepOnReading) {
            numberOfMessagesReadSoFar +=1;
            ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> oneRec : consumerRecords) {
                System.out.println("Key: " + oneRec.key() + "value : " + oneRec.value() +
                        "Partition: " + oneRec.partition() + "offset : " + oneRec.offset());

            }
            if(numberOfMessagesReadSoFar > numberOfMessagesToRead){
                keepOnReading = false;
                break;
            }
        }
    }
    }

