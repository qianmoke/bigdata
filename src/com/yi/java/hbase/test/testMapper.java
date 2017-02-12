package com.yi.java.hbase.test;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.io.*;
import org.apache.hadoop.hbase.client.Result;
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.util.Bytes;
import com.google.common.primitives.*;
//import com.yammer.metrics.core.HealthCheck.Result;
@SuppressWarnings("unused")
public class testMapper extends TableMapper<Text, IntWritable> {
public void map(ImmutableBytesWritable rowKey, Result columns, Context
context)
throws IOException, InterruptedException {
try {
// get rowKey and convert it to string
String inKey = new String(rowKey.get());
// set new key having only date
String oKey = inKey.split("#")[0];
// get sales column in byte format first and then convert it to string
(as it is stored as string from hbase shell)
byte[] bSales = columns.getValue(Bytes.toBytes("cf1"), Bytes.
toBytes("sales"));
String sSales = new String(bSales);
Integer sales = new Integer(sSales);
// emit date and sales values
context.write(new Text(oKey), new IntWritable(sales));
} catch (RuntimeException e){
e.printStackTrace();
}
}
}