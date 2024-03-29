package com.dolen.bigdata.hbase.mr.reducer;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

/**
 *ImmutableBytesWritable:rowkey
 * Put：
 * NullWritable：什么也不做
 */
public class InserDataReducer extends TableReducer <ImmutableBytesWritable, Put, NullWritable>{

    @Override
    protected void reduce(ImmutableBytesWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
        // 运行reducer，增加数据

        for (Put put : values) {

        context.write(NullWritable.get(), put);
        }

    }
}
