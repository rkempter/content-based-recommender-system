package ch.epfl.advdb.milestone2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

public class ScoreReducer extends Reducer<IntWritable, IntWritable, IntWritable, FloatWritable>{

	HashMap<Integer, HashMap<Integer, Boolean>> movies = new HashMap<Integer, HashMap<Integer, Boolean>>();
	
	@SuppressWarnings("deprecation")
	public void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		
		Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
		
		String line = null;
		
		for(Path file : localFiles) {
			BufferedReader reader = new BufferedReader(new FileReader(file.toString()));
			while ((line = reader.readLine()) != null){
				HashMap<Integer, Boolean> users = new HashMap<Integer, Boolean>();
				String[] stringFeatures = line.trim().split(Constants.TEXT_SEPARATOR);
				
				int movieId = Integer.parseInt(stringFeatures[0]);
				movies.put(movieId, users);
				
				for(int i = 1; i < stringFeatures.length; i++)
					users.put(Integer.parseInt(stringFeatures[i]), true);
			}
			reader.close();
		}
	}
	
	public void reduce(IntWritable key, 
			Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		HashMap<Integer, Boolean> retrievedUsers = movies.get(key.get());
		
		System.out.println("=============");
		System.out.println("Movie: "+key.get());
		
		int retrieved = retrievedUsers.size();
		int relevant = 0;
		int intersection = 0;
		
		Iterator<IntWritable> iter = values.iterator();
		while(iter.hasNext()) {
			relevant++;
			int user = iter.next().get();
			
			if(retrievedUsers.containsKey(user)) {
				intersection++;
			}
		}
		
		System.out.println("Retrieved: "+retrieved);
		System.out.println("Relevant: "+relevant);
		System.out.println("Intersection: "+intersection);
			
		float precision = (float) intersection / retrieved;
		float recall = (float) intersection / relevant;
		
		float fScore = 2 * (precision * recall) / (precision + recall);
		
		context.write(key, new FloatWritable(fScore));
	}
}
