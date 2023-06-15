package com.atguigu.hbase;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class HBaseClientTest {

    @Test
    public void testPut() throws IOException {
        Table table = HBaseClient.createTable("t1");

        Put put1 = HBaseClient.createPut("1009", "f2", "age", "12");
        Put put2 = HBaseClient.createPut("1009", "f2", "name", "tom");
        Put put3 = HBaseClient.createPut("1009", "f2", "gender", "M");

        List<Put> puts = Arrays.asList(put1,put2,put3);

        table.put(puts);

        table.close();
    }
    @Test
    public void testGet() throws IOException {
        Table table = HBaseClient.createTable("t1");

        Get get = new Get(Bytes.toBytes("1009"));

        Result result = table.get(get);

        HBaseClient.printResult(result);

        table.close();
    }
}