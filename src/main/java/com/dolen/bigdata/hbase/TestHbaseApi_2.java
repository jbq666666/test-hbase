package com.dolen.bigdata.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

/*
    测试api
   //通过java 访问hbase数据库
 */
public class TestHbaseApi_2 {

    public static void main(String[] args) throws Exception{

        //0)获取配置对象，获取hbase对象
        Configuration conf = HBaseConfiguration.create();
        //1)获取hbase连接对象
        Connection conn = ConnectionFactory.createConnection(conf);

        Admin admin = conn.getAdmin();
        TableName tableName = TableName.valueOf("dolen:student");

        /*
        //3)删除表
        if(admin.tableExists(tableName)){
            //禁用表
            admin.disableTable(tableName);

            //删除表
            admin.deleteTable(tableName);
        }else {

            System.out.println("表不存在........");
        }
        */

        /*
        //4)删除数据
        Table table = conn.getTable(tableName);

        String rowkey ="1001";
        Delete delete =new Delete(Bytes.toBytes(rowkey));

        table.delete(delete);
        System.out.println("删除数据成功..........");
        */

        //5)查询数据
        Table table = conn.getTable(tableName);

        Scan scan = new Scan();

        //返回查询结果对象
        ResultScanner scanner = table.getScanner(scan);

        System.out.println("获取查询数据1111111"+scanner);

        for (Result r: scanner ) {
            for (Cell cell : r.rawCells()) {
                System.out.println("family：" + Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("column：" + Bytes.toString(CellUtil.cloneQualifier(cell)));
                System.out.println("value：" + Bytes.toString(CellUtil.cloneValue(cell)));
                System.out.println("value：" + Bytes.toString(CellUtil.cloneRow(cell)));
            }
        }
    }
}
