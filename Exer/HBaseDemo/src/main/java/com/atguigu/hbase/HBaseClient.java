package com.atguigu.hbase;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseClient {

    public static Put createPut(String rk,String cf, String cq,String value){
        Put put = new Put(Bytes.toBytes(rk));
        return put.addColumn(Bytes.toBytes(cf),Bytes.toBytes(cq),Bytes.toBytes(value));
    }

    public static Table createTable(String name) throws IOException {
        if(StringUtils.isBlank(name)){
            throw new RuntimeException("表名非法！");
        }
        Table table = conn.getTable(TableName.valueOf(name));
        return table;
    }

    private static Connection conn;

    static {
        try {
            conn = ConnectionFactory.createConnection();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void close() throws IOException {
        if (conn != null){
            conn.close();
        }
    }

    public static void printResult(Result result) {

        Cell[] cells = result.rawCells();
        for (Cell cell : cells) {
            System.out.println(
                    "rk:"+Bytes.toString(CellUtil.cloneRow(cell))+
                    ","+Bytes.toString(CellUtil.cloneFamily(cell))+":"+
                    Bytes.toString(CellUtil.cloneQualifier(cell))+",ts:"+
                            cell.getTimestamp() + ",value:" +
                    Bytes.toString(CellUtil.cloneRow(cell))

            );
        }

    }
}
