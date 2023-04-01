package com.e2mg.bigdata.hadoop.writable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 描述
 *
 * @author EdiwalMusk
 * @date 2023/4/1 6:57
 */
public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    private FlowBean outV = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        long up = 0;
        long down = 0;
        for (FlowBean value : values) {
            up += value.getUpFlow();
            down += value.getDownFlow();
        }
        outV.setUpFlow(up);
        outV.setDownFlow(down);
        outV.setSumFlow();
        context.write(key, outV);
    }
}
