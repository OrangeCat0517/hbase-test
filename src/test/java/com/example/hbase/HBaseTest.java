package com.example.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;

public class HBaseTest {
    private static Connection connection; //Java到HBase的连接
    private static Admin admin; //所有和管理相关的操作，比如建立表格等，不涉及数据

    @BeforeAll
    public static void setUp() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost:2181,localhost:2182,localhost:2183");
        try {
            connection = ConnectionFactory.createConnection(conf);
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @AfterAll
    public static void down() {
        try {
            admin.close();
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    void list() {
        //测试list所有的表格
        try {
            for (TableName name : admin.listTableNames())
                System.out.println(name);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    void create() {
        //测试建立HBase中的表格
        try {
            TableName tableName = TableName.valueOf("scores");

            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }

            ColumnFamilyDescriptor grade =
                    ColumnFamilyDescriptorBuilder.newBuilder("grade".getBytes()).build();
            //ColumnFamilyDescriptor封装了所有和列族相关的信息，其中grade是列族的名字，build用于产生新的列族

            ColumnFamilyDescriptor course =
                    ColumnFamilyDescriptorBuilder.newBuilder("course".getBytes()).build();

            TableDescriptor scores = TableDescriptorBuilder.newBuilder(tableName)
                    .setColumnFamily(grade)     //为这个表格添加列族grade
                    .setColumnFamily(course)    //为这个表格添加列族course
                    .build();
            //TableDescriptor封装了所有和表格相关的信息

            admin.createTable(scores);  //建立表格

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    void desc() {
        try {
            TableName tableName = TableName.valueOf("scores");

            if (admin.tableExists(tableName)) {
                TableDescriptor tableDescriptor = admin.getDescriptor(tableName);
                ColumnFamilyDescriptor[] columnFamilyDescriptors = tableDescriptor.getColumnFamilies();
                for (ColumnFamilyDescriptor columnFamilyDescriptor : columnFamilyDescriptors)
                    System.out.println(columnFamilyDescriptor);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    void put() {
        TableName tableName = TableName.valueOf("scores");
        try {
            Table scores = connection.getTable(tableName);
            Put tom = new Put("tom".getBytes()); //准备好Put，此处的tom是row key
            tom.addColumn("grade".getBytes(), "".getBytes(), Bytes.toBytes("1"));
            tom.addColumn("course".getBytes(), "art".getBytes(), Bytes.toBytes("80"));
            tom.addColumn("course".getBytes(), "math".getBytes(), Bytes.toBytes("89"));

            Put jerry = new Put("jerry".getBytes());
            jerry.addColumn("grade".getBytes(), "".getBytes(), Bytes.toBytes("2"));
            jerry.addColumn("course".getBytes(), "art".getBytes(), Bytes.toBytes("87"));
            jerry.addColumn("course".getBytes(), "math".getBytes(), Bytes.toBytes("57"));

            scores.put(tom);
            scores.put(jerry);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    void scan() {
        try {
            TableName tableName = TableName.valueOf("scores");

            Table scores = connection.getTable(tableName);
            Scan scan = new Scan();
            ResultScanner resultsScanner = scores.getScanner(scan);
            for (Result result : resultsScanner) {
                System.out.println(result);
                for (Cell cell : result.listCells())
                    System.out.printf("Row key: %s, Family: %s, Qualifier: %s, Value: %s\n",
                            Bytes.toString(result.getRow()),
                            Bytes.toString(CellUtil.cloneFamily(cell)),
                            Bytes.toString(CellUtil.cloneQualifier(cell)),
                            Bytes.toString(CellUtil.cloneValue(cell)));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    void get() throws IOException {
        TableName tableName = TableName.valueOf("scores");
        Table scores = connection.getTable(tableName);
        Get get = new Get("tom".getBytes());
        get.addFamily("course".getBytes());
        Result result = scores.get(get);
        Cell[] cells = result.rawCells();
        for (Cell cell : cells)
            System.out.printf("Family: %s, Qualifier: %s, Value: %s\n",
                    Bytes.toString(CellUtil.cloneFamily(cell)),
                    Bytes.toString(CellUtil.cloneQualifier(cell)),
                    Bytes.toString(CellUtil.cloneValue(cell)));
    }

    @Test
    void delete() {
        TableName tableName = TableName.valueOf("scores");
        Delete delete = new Delete("tom".getBytes());
        try {
            Table scores = connection.getTable(tableName);
            scores.delete(delete);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Test
    void drop() {
        try {
            TableName tableName = TableName.valueOf("scores");

            if (admin.tableExists(tableName)) {
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
