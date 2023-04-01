package com.e2mg.bigdata.hadoop.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * 描述
 *
 * @author EdiwalMusk
 * @date 2023/4/1 7:57
 */
public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {
    private Text outK = new Text();
    private String finalName;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) context.getInputSplit();
        finalName = split.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1.获取一行
        String line = value.toString();
        // 2.切割
        String[] words = line.split(" ");
        if (finalName.contains("order")) {
            TableBean bean = new TableBean();
            bean.setId(words[0]);
            bean.setPid(words[1]);
            bean.setPname("");
            bean.setFlag("order");
            outK.set(words[1]);
            context.write(outK, bean);
        } else {
            TableBean bean = new TableBean();
            bean.setId("");
            bean.setPid(words[0]);
            bean.setPname(words[1]);
            bean.setFlag("product");
            outK.set(words[0]);
            context.write(outK, bean);
        }
    }
}