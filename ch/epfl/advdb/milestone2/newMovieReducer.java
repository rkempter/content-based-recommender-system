package ch.epfl.advdb.milestone2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

public class newMovieReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text>{
	
	protected IntWritable outputKey = new IntWritable();
	protected IntWritable outputValue = new IntWritable();
	
	private Hashtable<Integer, Integer> mapping = new Hashtable<Integer, Integer>();

	@SuppressWarnings({ "deprecation", "unchecked" })
	public void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		
		Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
		
		String line = null;
		
		for(Path file : localFiles) {
			
			// Check if file contains word centroid
			if(!file.toString().matches(".*(mapping).*"))
				continue;
			
			BufferedReader reader = new BufferedReader(new FileReader(file.toString()));
			while ((line = reader.readLine()) != null){
				
				String[] stringMappings = line.trim().split(Constants.TEXT_SEPARATOR);
				
				if(stringMappings.length != 2)
					new Exception("Problem with reading the file. Not correct format!");
				
				mapping.put(Integer.parseInt(stringMappings[0]), Integer.parseInt(stringMappings[1]));
			}
			reader.close();
		}
	}
	
	public void reduce(IntWritable key, 
			Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		Iterator<IntWritable> iter = values.iterator();
		String movies = "";
		if(iter.hasNext())
			movies = String.valueOf(iter.next().get());
		
		while(iter.hasNext()) {
			movies += Constants.TEXT_SEPARATOR + iter.next().get();	
		}
	
		int newKey = mapping.get(key.get());
		
		context.write(new IntWritable(newKey), new Text(movies));
	}
}
