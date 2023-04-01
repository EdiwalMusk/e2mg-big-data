package com.e2mg.bigdata.hadoop.writable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 描述
 *
 * @author EdiwalMusk
 * @date 2023/4/1 7:57
 */
public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    private Text outK = new Text();
    private FlowBean outV = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // super.map(key, value, context);

        // 1.获取一行
        String line = value.toString();

        // 2.切割
        String[] words = line.split(" ");

        // 3.循环写出
        String phone = words[0];
        long up = Integer.valueOf(words[1]);
        long down = Integer.valueOf(words[2]);

        outV.setUpFlow(up);
        outV.setDownFlow(down);
        outV.setSumFlow();

        outK.set(phone);
        context.write(outK, outV);
    }
}