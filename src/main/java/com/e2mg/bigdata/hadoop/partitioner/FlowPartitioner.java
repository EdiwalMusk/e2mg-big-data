package com.e2mg.bigdata.hadoop.partitioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 描述
 *
 * @author EdiwalMusk
 * @date 2023/4/1 11:38
 */
public class FlowPartitioner extends Partitioner<FlowBean, Text> {

    public int getPartition(FlowBean key, Text value, int numPartitions) {
        return (int) (key.getSumFlow() % 10);
    }
}
