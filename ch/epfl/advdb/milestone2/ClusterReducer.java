package ch.epfl.advdb.milestone2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

public class ClusterReducer extends 
	Reducer<IntWritable, FeatureWritable, Text, FeatureWritable>{
	
	int featureDimensions;
	String matrixType;
	
	public void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		
		featureDimensions = conf.getInt(Constants.FEATURE_DIMENSION_STRING, Constants.NUM_OF_FEATURES);
		
		matrixType = "I";
	}
	
	public void reduce(IntWritable key, 
			Iterable<FeatureWritable> values, Context context) throws IOException, InterruptedException {
		
		System.out.println("Key: "+key.get());
		
		float[] centroid = new float[featureDimensions];
		HashMap<Integer, Boolean> movies = new HashMap<Integer, Boolean>();
		
		Iterator<FeatureWritable> iterator = values.iterator();
		
		FeatureWritable movie;
		
		while(iterator.hasNext()) {
			movie = iterator.next();
			movies.put(movie.getId(), true);
			int featureNumber = movie.getFeatureNumber() - 1;
			System.out.println("Feature Number: "+featureNumber);
			System.out.println("Feature Value: "+movie.getFeatureValue());
			System.out.println("Centroid at this point: "+centroid[featureNumber]);
			System.out.println("Centroid lnegth = "+centroid.length);
			centroid[featureNumber] += movie.getFeatureValue();
		}
		
		int size = movies.size();
		
		for(int feature = 0; feature < centroid.length; feature++) {
			centroid[feature] = centroid[feature] / size;
			context.write(new Text(matrixType), new FeatureWritable(key.get(), feature, centroid[feature]));
		}
		
		
	}
	
}
