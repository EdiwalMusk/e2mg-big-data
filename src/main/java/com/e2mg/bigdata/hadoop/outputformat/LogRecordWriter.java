package com.e2mg.bigdata.hadoop.outputformat;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * 描述
 *
 * @author EdiwalMusk
 * @date 2023/4/1 12:01
 */
public class LogRecordWriter extends RecordWriter<Text, NullWritable> {
    private FSDataOutputStream os1;
    private FSDataOutputStream os2;

    public LogRecordWriter(TaskAttemptContext job) {
        try {
            FileSystem fs = FileSystem.get(job.getConfiguration());
            os1 = fs.create(new Path("/home/workdir/logs/out_1.log"));
            os2 = fs.create(new Path("/home/workdir/logs/out_2.log"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void write(Text key, NullWritable value) throws IOException, InterruptedException {
        if (key.toString().contains("a")) {
            os1.writeBytes(key.toString() + "\n");
        } else {
            os2.writeBytes(key.toString() + "\n");
        }
    }

    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.close(os1);
        IOUtils.close(os2);
    }
}
