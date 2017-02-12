//This is a Reducer module IdentityReducer.java created in the com.apress.hadoop.examples.ch2 package.
package com.yi.java.hadoop.word;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.MapReduceBase;
/** Performs no reduction, writing all input values directly to the
output. */
public class IdentityReducer<K, V> extends MapReduceBase implements Reducer<K, V, K, V> {
/** Writes all keys and values directly to output. */
	public void reduce(K key, Iterator<V> values, OutputCollector<K, V> output, Reporter reporter) throws IOException {
		while (values.hasNext()) {
			output.collect(key, values.next());
		}
	}
}