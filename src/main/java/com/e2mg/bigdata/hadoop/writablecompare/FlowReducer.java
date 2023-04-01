package com.e2mg.bigdata.hadoop.writablecompare;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 描述
 *
 * @author EdiwalMusk
 * @date 2023/4/1 6:57
 */
public class FlowReducer extends Reducer<FlowBean, Text, Text, FlowBean> {

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            System.out.println(value.toString());
            context.write(value, key);
        }
    }
}
