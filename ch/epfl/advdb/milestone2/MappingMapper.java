package ch.epfl.advdb.milestone2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

public class MappingMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
	
	private ArrayList<Hashtable<Integer, Boolean>> vClusters = new ArrayList<Hashtable<Integer, Boolean>>();
	
	@SuppressWarnings({ "deprecation", "unchecked" })
	public void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		
		Path localFiles[] = DistributedCache.getLocalCacheFiles(conf);
		
		String line = null;
		
		for(Path file : localFiles) {
			
			BufferedReader reader = new BufferedReader(new FileReader(file.toString()));
			while ((line = reader.readLine()) != null){
				
				String[] stringMovies = line.trim().split(Constants.TEXT_SEPARATOR);
				
				int cluster = Integer.parseInt(stringMovies[0]);
				
				while(cluster >= vClusters.size()) {
					Hashtable<Integer, Boolean> movies = new Hashtable<Integer, Boolean>();
					vClusters.add(movies);
				}
				
				for(int i = 1; i < stringMovies.length; i++) {
					vClusters.get(cluster).put(Integer.parseInt(stringMovies[i]), true);
				}
			}
			reader.close();
		}
	}

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] stringMovies = value.toString().trim().split(Constants.TEXT_SEPARATOR);
		
		int cluster = Integer.parseInt(stringMovies[0]);
		
		int bestMapping = 0;
		float bestJaccardCoefficient = 0;
		
		for(int j = 0; j < vClusters.size(); j++) {
			Hashtable<Integer, Boolean> vCentroid = vClusters.get(j);
			
			int vCentroidSize = vCentroid.size();
			int imdbCentroidSize = stringMovies.length - 1;
			int m11 = 0;
			
			for(int i = 1; i < stringMovies.length; i++) {
				int movie = Integer.parseInt(stringMovies[i]);
				
				if(vCentroid.containsKey(movie)) {
					m11++;
				}
			}
			
			int m01 = vCentroidSize - m11;
			int m10 = imdbCentroidSize - m11;
			
			float jaccardCoefficient = (float) m11 / (m10 + m01 + m11);
			
			if(jaccardCoefficient > bestJaccardCoefficient) {
				bestJaccardCoefficient = jaccardCoefficient;
				bestMapping = j;
			}
		}
		
		context.write(new IntWritable(cluster), new IntWritable(bestMapping));
	}

}
