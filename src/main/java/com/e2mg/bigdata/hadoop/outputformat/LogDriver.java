package com.e2mg.bigdata.hadoop.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
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
public class LogDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1.获取job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 2.设置jar路径
        job.setJarByClass(LogDriver.class);

        // 3.设置mapper
        job.setMapperClass(LogMapper.class);

        // 4.设置reducer
        job.setReducerClass(LogReducer.class);

        // 5.设置输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 6.设置最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        job.setOutputFormatClass(LogOutputFormat.class);

        // 7.设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 8.提交job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
