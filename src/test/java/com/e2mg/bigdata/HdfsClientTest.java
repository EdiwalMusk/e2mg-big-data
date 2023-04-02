package com.e2mg.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * 描述
 *
 * @author EdiwalMusk
 * @date 2023/4/2 9:01
 */
public class HdfsClientTest {
    private static FileSystem fs;

    @BeforeClass
    public static void setUp() throws URISyntaxException, IOException {
        URI uri = new URI("hdfs://192.168.94.194:8020");
        Configuration conf = new Configuration();
        // 指定访问hadoop的用户
        System.setProperty("HADOOP_USER_NAME", "root");
        fs = FileSystem.get(uri, conf);
        System.out.println("set up!");
    }

    @Test
    public void testMkdir() throws IOException {
        // 创建目录
        fs.mkdirs(new Path("/logs2"));
    }

    @Test
    public void testCopyFile() throws IOException {
        // 创建目录
        fs.copyFromLocalFile(false, true, new Path("D:\\02_workspace\\text\\123.txt"),
                new Path("/home/workdir/2.txt"));
    }

    @AfterClass
    public static void close() throws IOException {
        fs.close();
    }
}
