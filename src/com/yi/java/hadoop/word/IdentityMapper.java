//This is a Mapper module IdentityMapper.java created in com.apress.hadoop.examples.ch2 package.
package com.yi.java.hadoop.word;
import java.io.IOException;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.MapReduceBase;
/** Implements the identity function, mapping inputs directly to
outputs. */
public class IdentityMapper<K, V> extends MapReduceBase implements Mapper<K, V, K, V> {
/** The identify function. Input key/value pair is written directly
to * output.*/
	public void map(K key, V val, OutputCollector<K, V> output, Reporter reporter)
			throws IOException {
	output.collect(key, val);
	}
}