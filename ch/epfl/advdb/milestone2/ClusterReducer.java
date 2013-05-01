package ch.epfl.advdb.milestone2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.ftpserver.ftplet.Configuration;
import org.apache.ftpserver.ftplet.FtpException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

public class ClusterReducer extends 
	Reducer<IntWritable, MovieWritable, CentroidWritable, ClusterMoviesWritable>{
	
	int featureDimensions;
	
	public void setup(Context context) {
		Configuration conf = (Configuration) context.getConfiguration();
		
		featureDimensions = conf.getInt(Constants.FEATURE_DIMENSION_STRING, Constants.NUM_OF_FEATURES);
	}
	
	public void reduce(IntWritable key, 
			Iterable<MovieWritable> values, Context context) {
		
		Float[] centroid = new Float[featureDimensions];
		HashMap<Integer, Boolean> movies = new HashMap<Integer, Boolean>();
		
		Iterator<MovieWritable> iterator = values.iterator();
		
		MovieWritable movie;
		
		while(iterator.hasNext()) {
			movie = iterator.next();
			movies.put(movie.getMovie(), true);
			int featureNumber = movie.getFeatureNumber();
			centroid[featureNumber] += movie.getFeatureValue();
		}
		
		int size = movies.size();
		
		for(int feature = 0; feature < centroid.length; feature++) {
			centroid[feature] = centroid[feature] / size;
		}
		
	}
	
}
