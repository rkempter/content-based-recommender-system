package ch.epfl.advdb.milestone2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

public class ClusterReducer extends 
	Reducer<IntWritable, MovieWritable, CentroidWritable, ClusterMoviesWritable>{
	
	public void reduce(IntWritable key, 
			Iterable<MovieWritable> values, Context context) {
		
		// @todo: Num of features is sometimes only 10 (for V!)
		Float[] centroid = new Float[Constants.NUM_OF_FEATURES];
		
		ArrayList<Integer> movieList = new ArrayList<Integer>();
		
		Iterator<MovieWritable> iterator = values.iterator();
		int size = 0;
		
		while(iterator.hasNext()) {
			size++;
			features = iterator.next().getFeatures();
			
			for(IntFloatPairWritable feature : features) {
				centroid[feature.getKey()] += feature.getValue();
			}
		}
		
		for(int feature = 0; feature < centroid.length; feature++) {
			centroid[feature] = centroid[feature] / size;
		}
		
	}
	
}
