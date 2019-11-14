package com.dolen.bigdata.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.BasicConfigurator;

/*
    测试api
 */
public class TestHbaseApi_1 {

    public static void main(String[] args) throws Exception{
        BasicConfigurator.configure();
        //通过java 访问hbase数据库

        //0)获取配置对象，获取hbase对象
        Configuration conf = HBaseConfiguration.create();

        //1)获取hbase连接对象
        //ClassLoader
        // hbase-default.xml   hbase-site.xml
        Connection conn = ConnectionFactory.createConnection(conf);
//        System.out.println(conn);

        //2)获取操作对
        Admin admin= conn.getAdmin();

        //3)操作数据库
        //3-1)判断命名空间
        try{
            admin.getNamespaceDescriptor("dolen");
        }catch(NamespaceNotFoundException e){
            //若发生特定的异常，即找不到命名空间，则创建命名空间
            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create("dolen").build();
            admin.createNamespace(namespaceDescriptor);
        }

        //判断表是否存在
        TableName newTable = TableName.valueOf("dolen:student");
        boolean flg= admin.tableExists(newTable);
        System.out.println(flg);
        if (flg){
            // 查询数据
            Table table = conn.getTable(newTable);
            String rowkey = "1001";
            //string==>byte[]
            Get get =new Get(Bytes.toBytes(rowkey));
            //查询结果
            Result r = table.get(get);
            boolean empty = r.isEmpty();
            System.out.println("是否为空:"+empty);

            if(empty){
                System.out.println("没有查询到结果");
                // 新增数据
                Put put =new Put(Bytes.toBytes(rowkey));
                String family ="info";  //列族
                String column ="name";  //列
                String value ="zhangsan";//值
                put.addColumn(Bytes.toBytes(family),Bytes.toBytes(column),Bytes.toBytes(value) );

                table.put(put);
                System.out.println("增加数据成功");
            }else {
                //展示数据
                for (Cell cell: r.rawCells()){
                    CellUtil.cloneFamily(cell);
                    System.out.println("family = "+Bytes.toString(CellUtil.cloneFamily(cell)));
                    System.out.println("rowkey = "+Bytes.toString(CellUtil.cloneRow(cell)));
                    System.out.println("column = "+Bytes.toString(CellUtil.cloneQualifier(cell)));
                    System.out.println("key = "+Bytes.toString(CellUtil.cloneValue(cell)));
                }
                System.out.println("查询到结果");

            }
        }else{
            //创建表
            //创建表空间描述对象
            HTableDescriptor td = new HTableDescriptor(newTable);

            //创建列族
            HColumnDescriptor cd = new HColumnDescriptor("info");
            td.addFamily(cd);

            admin.createTable(td);
            System.out.println("表格创建成功........");
        }

        //4)获取操作结果


        //5)关闭数据库连接
        conn.close();
    }
}
