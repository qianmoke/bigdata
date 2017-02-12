package com.yi.java.mapreduce.wordcount;
//This is the Driver module, i.e. WordCountDriver.java.
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
public class WordCountDriver extends Configured implements Tool{
public static void main(String[] args) throws Exception
{
ToolRunner.run(new WordCountDriver(),args);
}
@Override
public int run(String[] args) throws Exception {
Job job = new Job(getConf(),"Basic Word Count Job");
job.setJarByClass(WordCountDriver.class);
//Map and Reduce
job.setMapperClass(WordCountMapper.class);
job.setReducerClass(WordCountReducer.class);
job.setNumReduceTasks( 1 );
job.setInputFormatClass(TextInputFormat.class);
//the map output
job.setMapOutputKeyClass(Text.class);
job.setMapOutputValueClass(IntWritable.class);
//the reduce output
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(IntWritable.class);
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
job.waitForCompletion(true);
return 0;
}
}