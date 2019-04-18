package com.cloudera;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.security.UserGroupInformation;

import javax.security.auth.Subject;
import java.io.IOException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;

public class HdfsViewer {

    public static void main(String[] args) throws Exception {
        test1();
    }

    //kerberos
    public static void test1() throws Exception {
        //设置java安全krb5配置，其中krb5.conf文件可以从成功开启kerberos的集群任意一台节点/etc/krb5.conf拿到,
        //这里应该也可以直接设置一下两个属性获取 ，我没有测试这个
        System.setProperty("java.security.krb5.realm","JIMI.COM");
        System.setProperty("java.security.krb5.KDC","master126");
        System.setProperty("java.security.krb5.conf", "D:/kafkaproducer/KafkaKerberosProducer/src/main/resources/krb5.conf");
        Configuration conf = new Configuration();
        //这里设置namenode新
        conf.set("fs.defaultFS", "hdfs://master126:8020");
        //需要增加hadoop开启了安全的配置
        conf.setBoolean("hadoop.security.authorization", true);
        //配置安全认证方式为kerberos
        conf.set("hadoop.security.authentication", "Kerberos");
        //设置namenode的principal
        conf.set("dfs.namenode.kerberos.principal", "hdfs/_HOST@JIMI.COM");
        //设置datanode的principal值为“hdfs/_HOST@YOU-REALM.COM”
        conf.set("dfs.datanode.kerberos.principal", "hdfs/_HOST@JIMI.COM");
        //通过hadoop security下中的 UserGroupInformation类来实现使用keytab文件登录
        UserGroupInformation.setConfiguration(conf);
        //设置登录的kerberos principal和对应的keytab文件，其中keytab文件需要kdc管理员生成给到开发人员
        UserGroupInformation.loginUserFromKeytab("hdfs/master126@JIMI.COM", "D:\\kafkaproducer\\KafkaKerberosProducer\\src\\main\\resources\\hdfs.keytab");
        //获取带有kerberos验证的文件系统类
        FileSystem fileSystem1 = FileSystem.get(conf);
        //测试访问情况
        Path path = new Path("hdfs://master126:8020/user/hdfs");
        if (fileSystem1.exists(path)) {
            System.out.println("===contains===");
        }
        RemoteIterator<LocatedFileStatus> list = fileSystem1.listFiles(path, true);
        while (list.hasNext()) {
            LocatedFileStatus fileStatus = list.next();
            System.out.println(fileStatus.getPath());
        }
    }
}