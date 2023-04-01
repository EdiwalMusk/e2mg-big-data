package com.e2mg.bigdata.hadoop.partitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 描述
 *
 * @author EdiwalMusk
 * @date 2023/4/1 6:57
 */
public class FlowDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1.获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2.设置jar路径
        job.setJarByClass(FlowDriver.class);

        // 3.设置mapper
        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        // 5.设置输出类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        // 6.设置最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 7.设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.out.println(args[0]);
        System.out.println(args[1]);

        job.setPartitionerClass(FlowPartitioner.class);
        job.setNumReduceTasks(10);

        // 8.提交job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
