package com.yi.java.mapreduce.wordcount;
//This is the Mapper Module, i.e. WordCountMapper.java.
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 *
 * @author training
 * Class: WordCountMapper
 **/
public class WordCountMapper extends Mapper<LongWritable, Text,
Text, IntWritable>{
/**
 * Optimization: Instead of creating the variables in the
 */
@Override
public void map(LongWritable inputKey,Text inputVal,Context
context) throws IOException, InterruptedException
{
String line = inputVal.toString();
String[] splits = line.trim().split("\\W+");
for(String outputKey:splits)
{
context.write(new Text(outputKey), new IntWritable(1));
}
}
}