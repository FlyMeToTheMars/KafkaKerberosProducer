package com.jimi;

import org.apache.kafka.clients.consumer.*;

import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;

import java.util.Properties;

/**
 * Created by fayson on 2017/10/24.
 */

public class MyConsumer {

    private static String TOPIC_NAME = "flume_collector_data";

    public static void main(String[] args) {

        System.setProperty("java.security.krb5.conf", "/Volumes/Transcend/keytab/krb5.conf");

        System.setProperty("java.security.auth.login.config", "/Volumes/Transcend/keytab/jaas-cache.conf");

        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");

        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "master126:9092,datanode127:9020,datanode128:9020");

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "cloudera_mirrormaker");

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");

        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");

        props.put("security.protocol", "SASL_PLAINTEXT");

        props.put("sasl.kerberos.service.name", "kafka");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        TopicPartition partition0 = new TopicPartition(TOPIC_NAME, 0);

        TopicPartition partition1 = new TopicPartition(TOPIC_NAME, 1);

        TopicPartition partition2 = new TopicPartition(TOPIC_NAME, 2);

        consumer.assign(Arrays.asList(partition0, partition1, partition2));

        ConsumerRecords<String, String> records = null;

        while (true) {

            try {

                Thread.sleep(10000l);

                System.out.println();

                records = consumer.poll(Long.MAX_VALUE);

                for (ConsumerRecord<String, String> record : records) {

                    System.out.println("Receivedmessage: (" + record.key() + "," + record.value() + ") at offset " + record.offset());

                }

            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }
}