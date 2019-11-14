package com.dolen.bigdata.hbase.mr;

import com.dolen.bigdata.hbase.mr.tool.HbaseMapreduceTool;
import org.apache.hadoop.util.ToolRunner;
/*
*可运行jar
* */
public class Table2TableApplication {

    public static void main(String[] args) throws Exception {

    // ToolRunner 可以运行MR
    ToolRunner.run(new HbaseMapreduceTool(), args);
    }
}