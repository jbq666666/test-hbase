package com.dolen.bigdata.hbase;


import com.dolen.bigdata.hbase.util.HBaseUtil;
import org.apache.hadoop.hbase.client.Scan;

/*
    演示查询,过滤等功能,未完成
            */
public class TestHbaseApi_4 {

    public static void main(String[] args) throws Exception{


        Scan scan =new Scan();
        //scan.setFilter()


        System.out.println("添加数据成功...........");
        HBaseUtil.close();
    }
}
