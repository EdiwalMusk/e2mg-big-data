package com.e2mg.bigdata.hadoop.mapjoin;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 描述
 *
 * @author EdiwalMusk
 * @date 2023/4/1 7:57
 */
public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {
    private Text outK = new Text();
    private Map<String, String> pdMap = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        URI[] cacheFiles = context.getCacheFiles();

        FileSystem fs = FileSystem.get(context.getConfiguration());
        FSDataInputStream fis = fs.open(new Path(cacheFiles[0]));
        List<String> list = IOUtils.readLines(fis, "utf-8");
        list.forEach(o -> {
            String[] strings = o.split(" ");
            pdMap.put(strings[0], strings[1]);
        });
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1.获取一行
        String line = value.toString();
        // 2.切割
        String[] words = line.split(" ");
        TableBean bean = new TableBean();
        bean.setId(words[0]);
        bean.setPid(words[1]);
        bean.setPname(pdMap.get(words[1]));
        bean.setFlag("order");
        outK.set(words[0]);
        context.write(outK, bean);
    }
}