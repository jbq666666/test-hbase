package com.dolen.bigdata.hbase.mr.tool;

import com.dolen.bigdata.hbase.mr.mapper.ScanDataMapper;
import com.dolen.bigdata.hbase.mr.reducer.InserDataReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.util.Tool;

public class HbaseMapreduceTool implements Tool {

    public int run(String[] args) throws Exception {
        // 作业 job
        Job job = Job.getInstance();
//        Configuration con = new Configuration();
//        String s = con.get("fs.defaultFS", "hdfs://hxdfstest");
//        Job job = Job.getInstance(con);

        job.setJarByClass(HbaseMapreduceTool.class);

        //mapper
        //将查询的数据，变成一个一个put
        TableMapReduceUtil.initTableMapperJob(
                "dolen:student",
                new Scan(),
                ScanDataMapper.class,
                ImmutableBytesWritable.class,
                Put.class,
                job
                );

        //reducer
        TableMapReduceUtil.initTableReducerJob(
                "dolen:user",
                InserDataReducer.class,
                job
                );

        //等待作业完成
        boolean flag = job.waitForCompletion(true);

        return flag?JobStatus.State.SUCCEEDED.getValue() : JobStatus.State.FAILED.getValue();
    }

    public void setConf(Configuration conf) {

    }

    public Configuration getConf() {
        return null;
    }
}
