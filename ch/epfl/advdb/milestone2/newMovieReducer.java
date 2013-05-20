package ch.epfl.advdb.milestone2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
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
	
	private Hashtable<Integer, ArrayList<Integer>> mapping = new Hashtable<Integer, ArrayList<Integer>>();

	@SuppressWarnings({ "deprecation", "unchecked" })
	public void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		
		Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
		
		String line = null;
		
		for(Path file : localFiles) {
			
			// Check if file contains word centroid
			if(file.toString().matches(".*(_centroids_).*"))
				continue;
			
			BufferedReader reader = new BufferedReader(new FileReader(file.toString()));
			while ((line = reader.readLine()) != null){
				
				String[] stringMappings = line.trim().split(Constants.TEXT_SEPARATOR);
				
				if(stringMappings.length != 2)
					new Exception("Problem with reading the file. Not correct format!");
				
				int iCluster = Integer.parseInt(stringMappings[0]);
				int vCluster = Integer.parseInt(stringMappings[1]);
				
				if(mapping.containsKey(iCluster)) {
					mapping.get(iCluster).add(vCluster);
				} else {
					ArrayList<Integer> newList = new ArrayList<Integer>();
					newList.add(vCluster);
					mapping.put(iCluster, newList);
				}
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
	
		ArrayList<Integer> newKeys = mapping.get(key.get());
		
		for(Integer newKey : newKeys) {
			context.write(new IntWritable(newKey), new Text(movies));
		}
		
	}
}
