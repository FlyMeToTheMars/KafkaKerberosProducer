package com.cloudera;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class MyNewProducer {
    //发送topic
    public static String TOPIC_NAME = "flume_collector_data";

    public static void main(String[] args) {

        System.setProperty("java.security.krb5.conf",
                Thread.currentThread().getContextClassLoader().getResource("krb5.conf").getPath());
        //初始化jaas.conf文件
        MyProperties.configureJAAS(Thread.currentThread().getContextClassLoader().getResource("kafka.keytab").getPath(), "kafka/master126@JIMI.COM");
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");

        //System.setProperty("sun.security.krb5.debug","true");

        //初始化kerberos环境
        MyProperties props = MyProperties.initProducer();

        //kafka brokers地址
        props.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "master126:9092,datanode127:9092.datanode128:9092");

        org.apache.kafka.clients.producer.Producer<String, String> producer = new KafkaProducer<String, String>(
                props.getProperties());

        for (int i = 0; i < 10; i++) {

            String key = "key-" + i;

            String message = "Message-" + i;

            ProducerRecord record = new ProducerRecord<String, String>(
                    TOPIC_NAME, key, message);

            producer.send(record);

            System.out.println(key + "----" + message);

        }

        producer.close();

    }
}