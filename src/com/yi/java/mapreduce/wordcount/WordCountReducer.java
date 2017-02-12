package com.yi.java.mapreduce.wordcount;
//This is the Reducer Module, i.e. WordCountReducer.java.
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class WordCountReducer extends
Reducer<Text,IntWritable,Text, IntWritable>{
@Override
	public void reduce(Text key,Iterable<IntWritable> listOfValues,Context context) 
			throws IOException,InterruptedException {
		int sum=0;
		for(IntWritable val:listOfValues){
			sum = sum + val.get();
		}
		context.write(key,new IntWritable(sum));
	}
}