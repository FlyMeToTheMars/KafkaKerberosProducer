package com.cloudera;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

public class HdfsKerberosDemo {
    static Configuration conf = new Configuration();

    static {
        System.setProperty("java.security.krb5.conf","D:\\kafkaproducer\\KafkaKerberosProducer\\src\\main\\resources\\krb5.conf");
        conf.set("fs.defaultFS", "hdfs://master126:8020");
        conf.set("dfs.nameservices", "cluster");
    }

}
