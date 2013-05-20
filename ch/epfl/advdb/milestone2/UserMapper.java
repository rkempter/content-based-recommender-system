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
	
	private float[][] centroids;
	
	float max = 0;
	float min = 0;
	
	private Hashtable<Integer, ArrayList<Integer>> clusterMovieMapping = new Hashtable<Integer, ArrayList<Integer>>();
	
	private HashMap<Integer, Boolean> users = new HashMap<Integer, Boolean>();
	
	private int numberOfClusters;
	
	protected IntWritable outputKey = new IntWritable();
	protected IntWritable outputValue = new IntWritable();
	
	@SuppressWarnings("deprecation")
	public void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		
		numberOfClusters = conf.getInt(Constants.NBR_CLUSTERS, 10);
		
		centroids = new float[numberOfClusters][10];
		
		localFiles = DistributedCache.getLocalCacheFiles(conf);
		
		String line = null;
		
		// Two types of files: V_Clusters & <cluster, movies> files
		for(Path file : localFiles) {
			BufferedReader reader = new BufferedReader(new FileReader(file.toString()));
			while ((line = reader.readLine()) != null){
				String[] stringFeatures = line.trim().split(Constants.TEXT_SEPARATOR);
				
				int clusterId = 0;
				if(file.toString().matches(".*(_centroids_).*")) {
					clusterId = Integer.parseInt(stringFeatures[1]);
					centroids[clusterId][Integer.parseInt(stringFeatures[2])] = Float.parseFloat(stringFeatures[3]);
				} else {
					clusterId = Integer.parseInt(stringFeatures[0]);
					if(clusterMovieMapping.containsKey(clusterId)) {
						ArrayList<Integer> nextMovies = readMovies(stringFeatures);
						clusterMovieMapping.get(clusterId).addAll(nextMovies);
					} else {
						clusterMovieMapping.put(clusterId, readMovies(stringFeatures));
					}
				}	
			}
			reader.close();
		}
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		// Convert value into vector
		String[] uValuesString = value.toString().trim().split(Constants.TEXT_SEPARATOR);
		
		float[] uValues = Utility.readUValues(uValuesString);
		int userId = Integer.parseInt(uValuesString[0]);
		
		for(Map.Entry<Integer, ArrayList<Integer>> entry : clusterMovieMapping.entrySet()) {
				
			int cluster = entry.getKey();
				
			float[] centroidValues = centroids[cluster];
				
			float userSum = Utility.getUserRating(centroidValues, uValues);
				
				
			if(userSum > 0 & userSum <= 2.5) {
				for(Integer movie : entry.getValue()) {
					outputKey.set(movie);
						
					outputValue.set(userId);
					context.write(outputKey, outputValue);
				}
			}
				
			if(userSum > max) {
				max = userSum;
			} else if(userSum < min) {
				min = userSum;
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
}
