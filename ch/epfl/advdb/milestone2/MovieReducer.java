package ch.epfl.advdb.milestone2;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class MovieReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text>{

	
	public void reduce(IntWritable key, 
			Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		Iterator<IntWritable> iter = values.iterator();
		String movies = "";
		if(iter.hasNext())
			movies = String.valueOf(iter.next().get());
		
		while(iter.hasNext()) {
			movies += Constants.TEXT_SEPARATOR + iter.next().get();	
		}
		
		context.write(key, new Text(movies));
	}
}
