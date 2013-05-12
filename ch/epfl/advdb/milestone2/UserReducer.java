package ch.epfl.advdb.milestone2;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class UserReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text>{

	
	public void reduce(IntWritable key, 
			Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		Iterator<IntWritable> iter = values.iterator();
		StringBuffer users = new StringBuffer();
		if(iter.hasNext())
			users.append(String.valueOf(iter.next().get()));
		
		while(iter.hasNext()) {
			users.append(Constants.TEXT_SEPARATOR);
			users.append(iter.next().get());
		}
		
		context.write(key, new Text(users.toString()));
	}
}
