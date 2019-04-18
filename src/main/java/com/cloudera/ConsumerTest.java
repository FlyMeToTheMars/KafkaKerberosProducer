package com.cloudera;

import java.util.Arrays;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerTest {
    public static void main(String[] args) {

        System.setProperty("java.security.auth.login.config", "D:\\kafkaproducer\\KafkaKerberosProducer\\src\\main\\resources\\jaas.conf");
        System.setProperty("java.security.krb5.conf", Thread.currentThread()
                .getContextClassLoader().getResource("krb5.conf").getPath());
        // 环境变量添加，需要输入配置文件的路径System.out.println("===================配置文件地址"+fsPath+"\\conf\\cons_client_jaas.conf");
        Properties props = new Properties();
        props.put("bootstrap.servers", "master126:9092");
        props.put("group.id", "cloudera_mirrormaker");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "earliest");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "GSSAPI");
        props.put("sasl.kerberos.service.name", "kafka");

        KafkaConsumer kafkaConsumer = new KafkaConsumer(props);
        kafkaConsumer.subscribe(Arrays.asList("flume_collector_data"));
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(1);
            for (ConsumerRecord<String, String> record : records)
                System.out.println("Partition: " + record.partition() + " Offset: " + record.offset() + " Value: " + record.value() + " ThreadID: " + Thread.currentThread().getId());
        }
    }
}
