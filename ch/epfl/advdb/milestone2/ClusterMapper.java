package ch.epfl.advdb.milestone2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

public class ClusterMapper extends Mapper<LongWritable, Text, IntWritable, MovieWritable>{

	private Path[] localFiles;
	
	private ArrayList<Float[]> clusters;
	
	private IntWritable outputKey = new IntWritable();
	private MovieWritable outputValue = new MovieWritable();
	
	public void configure(JobConf job) throws IOException {
		localFiles = DistributedCache.getLocalCacheFiles(job);
		
		for(Path file : localFiles) {
			// Foreach line
			// new Float[Constants.NUM_OF_FEATURES] -> clusters
		}
	}
	
	public void map(LongWritable key, Text value, Context context) {
		
		HashMap<Integer, Float> featureVector = new HashMap<Integer, Float>();
		
		// Transform value into text and load into hashmap
		
		float maxSimilarity = 0;
		int optimalCluster = 0;
		
		for(int i = 0; i < clusters.size(); i++) {
			Float[] centroidFeatureVector = clusters.get(i);
			
			float centroidSize = getVectorSize(centroidFeatureVector);
			
			float featureVectorSize = 0;
			
			float cosineSimilarity = 0;
			
			for(int feature : featureVector.keySet()) {
				if(feature > centroidFeatureVector.length)
					new Exception("Feature is outside of centroid Feature vector");
				
				float vectorVal = featureVector.get(feature);
				float centroidVal = centroidFeatureVector[feature];
				
				cosineSimilarity += vectorVal * centroidVal;
				
				featureVectorSize += Math.pow(vectorVal, 2);
			}
			featureVectorSize = (float) Math.sqrt(featureVectorSize);
			
			cosineSimilarity = cosineSimilarity / (featureVectorSize * centroidSize);
			
			if(cosineSimilarity > maxSimilarity) {
				maxSimilarity = cosineSimilarity;
				optimalCluster = i;
			}
		}
		
		outputKey.set(optimalCluster);
		outputValue.set(featureVector.keySet());
		context.write(outputKey, outputValue);
	}
	
	private float getVectorSize(Float[] v) {
		float result = 0;
		
		for(int i = 0; i < v.length; i++) {
			result += Math.pow(v[i], 2);
		}
		
		return (float) Math.sqrt(result);
	}
}
