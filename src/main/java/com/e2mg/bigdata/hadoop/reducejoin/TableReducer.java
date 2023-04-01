package com.e2mg.bigdata.hadoop.reducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

/**
 * 描述
 *
 * @author EdiwalMusk
 * @date 2023/4/1 6:57
 */
public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {


    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        TableBean pdBean = new TableBean();
        List<TableBean> orderBeans = new ArrayList<TableBean>();
        for (TableBean value : values) {
            // hadoop迭代计算，导致value是相同地址，值会覆盖
            TableBean tmpBean = new TableBean();
            if (value.getFlag().contains("order")) {
                try {
                    BeanUtils.copyProperties(tmpBean, value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
                orderBeans.add(tmpBean);
            } else {
                try {
                    BeanUtils.copyProperties(pdBean, value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }
        for (TableBean value : orderBeans) {
            value.setPname(pdBean.getPname());
            context.write(value, NullWritable.get());
        }
    }
}
