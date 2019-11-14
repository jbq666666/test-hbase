package com.dolen.bigdata.hbase;


import com.dolen.bigdata.hbase.util.HBaseUtil;

/*
    使用同一个线程，共享数据，不能解决线程安全问题
 */
public class TestHbaseApi_3 {

    public static void main(String[] args) throws Exception{
        HBaseUtil.makeHbaseConnection();

        HBaseUtil.inserHbaseData("dolen:student", "1004", "info", "name", "zl");

        System.out.println("添加数据成功...........");
        HBaseUtil.close();
    }
}
