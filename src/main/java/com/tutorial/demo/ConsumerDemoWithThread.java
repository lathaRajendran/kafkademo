package com.tutorial.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

    private ConsumerDemoWithThread() {
    }

    private void run(){
        String topic = "topic_demo";
        String bootServer = "127.0.0.1:9092";
        String group = "my sixth group";
        CountDownLatch latch = new CountDownLatch(1);
//Create runnable
        Runnable myConsumerRunnable = new ConsumerRunnable(bootServer,
                topic, group, latch);
        Thread myThread= new Thread(myConsumerRunnable);
        //start runnable
        myThread.start();

        //Add a shut down hook
        Runtime.getRuntime().addShutdownHook(new Thread(()->
        {
            System.out.println("Caught shut down");
            ((ConsumerRunnable)myConsumerRunnable).shutDown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            System.out.println("Application has exited");
        }
        ));

        try {
            latch.await();
        }catch(InterruptedException e){
            System.out.println("App is interrupted");
        }finally{
            System.out.println("App is closing");
        }
    }

    public class ConsumerRunnable implements  Runnable{
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;

        public ConsumerRunnable(String bootServer,
                              String topic,
                              String group,
                              CountDownLatch latch){
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            consumer = new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Arrays.asList(topic));
            this.latch = latch;
        }

        @Override
        public void run(){
            //poll for new data
            try {
                while (true) {
                    ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> oneRec : consumerRecords) {
                        System.out.println("Key: " + oneRec.key() + "value : " + oneRec.value() +
                                "Partition: " + oneRec.partition() + "offset : " + oneRec.offset());

                    }
                }
            }catch(WakeupException e){
                System.out.println("Received Shutdown signal");
            }finally{
                consumer.close();
            }

        }

        public void shutDown(){
           consumer.wakeup();
           latch.countDown();
        }

    }
}
