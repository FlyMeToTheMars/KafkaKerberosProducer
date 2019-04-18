package com.cloudera;

import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;

public class MyNewConsumer {
    private static String TOPIC_NAME = "flume_collector_data";

    public static void main(String[] args) {

        System.setProperty("java.security.krb5.conf", Thread.currentThread()
                .getContextClassLoader().getResource("krb5.conf").getPath());
        //初始化jaas.conf文件
        MyProperties.configureJAAS(Thread.currentThread().getContextClassLoader().getResource("kafka.keytab").getPath(), "kafka/master126@JIMI.COM");

        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");

        MyProperties props = MyProperties.initKerberos();

        props.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "master126:9092");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(
                props.getProperties());
        consumer.subscribe(Arrays.asList(TOPIC_NAME));
        /*
         * TopicPartition partition0= new TopicPartition(TOPIC_NAME, 0);
         *
         * TopicPartition partition1= new TopicPartition(TOPIC_NAME, 1);
         *
         * TopicPartition partition2= new TopicPartition(TOPIC_NAME, 2);
         */

        // consumer.assign(Arrays.asList(partition0,partition1, partition2));

        ConsumerRecords<String, String> records = null;

        while (true) {
            try {
                Thread.sleep(1000);

                System.out.println();
                records = consumer.poll(Long.MAX_VALUE);

                for (ConsumerRecord<String, String> record : records) {

                    System.out.println("Receivedmessage: (" + record.key()
                            + "," + record.value() + ") at offset "
                            + record.offset());
                }

            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        /*
         * while (true){
         *
         * try {
         *
         * Thread.sleep(10000l);
         *
         * System.out.println();
         *
         * records = consumer.poll(Long.MAX_VALUE);
         *
         * for (ConsumerRecord<String, String> record : records) {
         *
         * System.out.println("Receivedmessage: (" + record.key() + "," +
         * record.value() + ") at offset " + record.offset());
         *
         * }
         *
         * } **catch** (**InterruptedException** e){
         *
         * e.printStackTrace();
         *
         * }
         */

    }
}
