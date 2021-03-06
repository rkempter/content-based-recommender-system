package ch.epfl.advdb.milestone2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

import ch.epfl.advdb.milestone2.DataClustering.CLUSTER_COUNTERS;

public class ClusterReducer extends 
	Reducer<IntWritable, FeatureWritable, Text, FeatureWritable>{

	String matrixType;
	int clusterType;
	
	public void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		
		// Know if netflix or imdb dataset is worked on
		this.clusterType = conf.getInt(Constants.MATRIX_TYPE, Constants.NETFLIX_CLUSTER);
		
		if(clusterType == Constants.NETFLIX_CLUSTER) {
			matrixType = "N";
		} else {
			matrixType = "I";
		}
	}
	
	public void reduce(IntWritable key, 
			Iterable<FeatureWritable> values, Context context) throws IOException, InterruptedException {
	
		HashMap<Integer, Float> centroid = new HashMap<Integer, Float>();
		HashMap<Integer, Boolean> movies = new HashMap<Integer, Boolean>();
		
		Iterator<FeatureWritable> iterator = values.iterator();
		
		FeatureWritable movie;
		
		while(iterator.hasNext()) {
			movie = iterator.next();
			// store movie in hashtable
			movies.put(movie.getId(), true);
			int featureNumber = movie.getFeatureNumber();
			float featureValue = movie.getFeatureValue();
			// add feature value to centroid for computing the mean
			if(centroid.containsKey(featureNumber))
				featureValue = centroid.get(featureNumber) + featureValue;
			centroid.put(featureNumber,featureValue);
		}
		
		int size = movies.size();
		float length = 0;
		
		// compute length
		for(Map.Entry<Integer, Float> entry : centroid.entrySet()) {
			entry.setValue(entry.getValue() / size);
			length += Math.pow(entry.getValue(),2);
		}
		
		for(Map.Entry<Integer, Float> entry : centroid.entrySet()) {
			// normalization
			float value = entry.getValue() / (float) Math.sqrt(length);
			
			// Convergence criterion
			if(!centroid.containsKey(entry.getKey())) {
				incrementCounter(context);
			} else {
				float diff = centroid.get(entry.getKey()) - value;
				if(diff > 0.00001) {
					incrementCounter(context);
				}
			}
			context.write(new Text(matrixType), new FeatureWritable(key.get(), entry.getKey(), value));
		}
	}
	
	// Counter incrementation for convergence criterion
	private void incrementCounter(Context context) {
		if(clusterType == Constants.NETFLIX_CLUSTER) {
			context.getCounter(CLUSTER_COUNTERS.NETFLIX_COUNTER).increment(1);
		} else {
			context.getCounter(CLUSTER_COUNTERS.IMDB_COUNTER).increment(1);
		}
	}
	
}
