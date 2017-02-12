package com.yi.java.hbase.test;
import java.io.IOException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.util.Bytes;
import com.google.common.primitives.*;
@SuppressWarnings("unused")
public class testReducer extends TableReducer<Text, IntWritable,
ImmutableBytesWritable>{
public void reduce(Text key, Iterable<IntWritable> values , Context
context)
throws IOException, InterruptedException {
try {
int sum = 0;
// loop through different sales vales and add it to sum
for (IntWritable sales : values) {
Integer intSales = new Integer(sales.toString());
sum += intSales;
}
String keyString=key.toString();
System.out.println(""+keyString+"\t"+sum);
// create hbase put with rowkey as date
Put insHBase = new Put(key.getBytes());
// insert sum value to hbase
insHBase.add(Bytes.toBytes("cf1"), Bytes.toBytes("Total sales:"),
Bytes.toBytes(sum));
// write data to Hbase table
context.write(null, insHBase);
} catch (Exception e) {
e.printStackTrace();
}
}