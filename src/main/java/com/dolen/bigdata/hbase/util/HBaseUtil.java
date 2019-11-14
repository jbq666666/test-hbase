package com.dolen.bigdata.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/*
* 创建hbase 连接池
* */
public class HBaseUtil {
    static ThreadLocal<Connection> threadLocal = new ThreadLocal<Connection>();

    /*
    * 获取连接
    * */
    public static void makeHbaseConnection() throws Exception{

        Connection conn = threadLocal.get();

        if(conn == null) {
            Configuration conf = HBaseConfiguration.create();
            conn = ConnectionFactory.createConnection(conf);

            threadLocal.set(conn);
        }
    }

    /*
    *插入数据
    * */
    public static void inserHbaseData(String tableName,String rowkey,String family,String column,String value) throws Exception{

        Connection conn = threadLocal.get();
        Table table = conn.getTable(TableName.valueOf(tableName));

       // (family, qualifier, value)
        byte[] rowKey = Bytes.toBytes(rowkey);
        byte[] f = Bytes.toBytes(family);
        byte[] c = Bytes.toBytes(column);
        byte[] v = Bytes.toBytes(value);

        Put put = new Put(rowKey);
        put.addColumn(f,c,v );

        table.put(put);
        table.close();
    }


    /*
    * 关闭连接
    * */
    public static void close() throws Exception{
        Connection conn =threadLocal.get();

        if(conn != null){
            //关闭连接
            conn.close();
            //删除threadLocal
            threadLocal.remove();
        }
    }
}
