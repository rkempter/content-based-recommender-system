package ch.epfl.advdb.milestone2;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

public class UserMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>{

	private Path[] localFiles;
	
	private Float[][] centroids;
	
	
	
	private Hashtable<Integer, ArrayList<Integer>> clusterMovieMapping = new Hashtable<Integer, ArrayList<Integer>>();
	
	private int numberOfClusters;
	
	protected IntWritable outputKey = new IntWritable();
	protected IntWritable outputValue = new IntWritable();
	
	@SuppressWarnings("deprecation")
	public void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		
		numberOfClusters = conf.getInt(Constants.NBR_CLUSTERS, 60);
		
		centroids = new Float[numberOfClusters][10];
		
		localFiles = DistributedCache.getLocalCacheFiles(conf);
		
		String line = null;
		
		// Two types of files: V_Clusters & <cluster, movies> files
		for(Path file : localFiles) {
			BufferedReader reader = new BufferedReader(new FileReader(file.toString()));
			while ((line = reader.readLine()) != null){
				String[] stringFeatures = line.trim().split(Constants.TEXT_SEPARATOR);
				
				int clusterId = Integer.parseInt(stringFeatures[0]);
				
				if(file.toString().matches(".*(_centroids_).*"))
					centroids[clusterId] = readDimensions(stringFeatures);
				else
					clusterMovieMapping.put(clusterId, readMovies(stringFeatures));
			}
			reader.close();
		}
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		// Convert value into vector
		String[] uValuesString = value.toString().trim().split(Constants.TEXT_SEPARATOR);

		// store userId
		int userId = Integer.parseInt(uValuesString[0]);
		
		float userSum = 0;
		
		for(Map.Entry<Integer, ArrayList<Integer>> entry : clusterMovieMapping.entrySet()) {
			int cluster = entry.getKey();
			
			Float[] centroidValues = centroids[cluster];
			for(int i = 0; i < 10; i++) {
				userSum += centroidValues[i] * Float.parseFloat(uValuesString[i]);
			}
			
			if(userSum > 0) {
				for(Integer movie : entry.getValue()) {
					outputKey.set(movie);
					outputValue.set(userId);
				}
			}
		}
	}
	
	private ArrayList<Integer> readMovies(String[] moviesString) {
		ArrayList<Integer> movies = new ArrayList<Integer>();
		for(int i = 1; i < moviesString.length; i++) {
			movies.add(Integer.parseInt(moviesString[i]));
		}
		
		return movies;
	}
	
	private Float[] readDimensions(String[] dimensionString) {
		Float[] dimensions = new Float[Constants.NUM_OF_NETFLIX_FEATURES];
		
		if(dimensionString.length != Constants.NUM_OF_NETFLIX_FEATURES)
			new Exception("not enough dimensions");
		
		for(int i = 1; i <= Constants.NUM_OF_NETFLIX_FEATURES; i++) {
			dimensions[i-1] = Float.parseFloat(dimensionString[i]);
		}
		
		return dimensions;
	}
}
