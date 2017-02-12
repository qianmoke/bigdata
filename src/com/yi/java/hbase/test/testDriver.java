package com.yi.java.hbase.test;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.hbase.util.Bytes;
import com.google.common.primitives.*;
@SuppressWarnings("unused")
public class testDriver {
public static void main(String[] args) throws Exception {
Configuration conf = new Configuration();
 // define scan and define column families to scan
Scan scan = new Scan();
scan.addFamily(Bytes.toBytes("cf1"));
Job job = new Job(conf);
job.setMapperClass(testMapper.class);
job.setReducerClass(testReducer.class);
job.setJarByClass(testDriver.class);
// define input hbase table
TableMapReduceUtil.initTableMapperJob(
"EMPLOYEE",
scan,
testMapper.class,
Text.class,
IntWritable.class,
job);
// define output table
TableMapReduceUtil.initTableReducerJob(
"TotalSale",
testReducer.class,
job);
job.waitForCompletion(true);
}
}