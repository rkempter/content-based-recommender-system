package ch.epfl.advdb.milestone2;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
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

public class MovieMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>{

	protected HashMap<Integer, Float>[] clusters;
	
	protected int numberOfClusters;
	protected int clusterType;
	
	protected IntWritable outputKey = new IntWritable();
	protected IntWritable outputValue = new IntWritable();
	
	@SuppressWarnings({ "deprecation", "unchecked" })
	public void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		
		numberOfClusters = conf.getInt(Constants.NBR_CLUSTERS, 10);
		clusterType = conf.getInt(Constants.MATRIX_TYPE, Constants.NETFLIX_CLUSTER);
		
		Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
		
		clusters = Utility.readCluster(new HashMap[numberOfClusters], localFiles);
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		ArrayList<FeatureWritable> features = new ArrayList<FeatureWritable>();
		
		String[] movie = value.toString().trim().split(Constants.TEXT_SEPARATOR);
		
		if(clusterType == Constants.IMDB_CLUSTER) {
			for(int i = 1; i < movie.length; i++) {
				features.add(new FeatureWritable(Integer.parseInt(movie[0]), Integer.parseInt(movie[i]), (float) 1));
			}
		} else {
			for(int i = 1; i < movie.length; i++) {
				features.add(new FeatureWritable(Integer.parseInt(movie[0]), i, Float.parseFloat(movie[i].trim())));
			}
		}
		
		if(features.size() > 0) {
			int optimalCluster = Utility.getBestCluster(clusters, features);
			
			outputKey.set(optimalCluster);
			outputValue.set(Integer.parseInt(movie[0]));
			context.write(outputKey, outputValue);
		}
	}
}
