package ch.epfl.advdb.milestone2;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

public class ClusterMapper extends Mapper<LongWritable, Text, IntWritable, FeatureWritable>{

	private Path[] localFiles;
	
	private ArrayList<HashMap<Integer, Float>> clusters;
	
	private IntWritable outputKey = new IntWritable();
	private FeatureWritable outputValue = new FeatureWritable();
	
	public void configure(JobConf job) throws IOException {
		localFiles = DistributedCache.getLocalCacheFiles(job);
		
		FileSystem fs = FileSystem.get(job);
		String line = null;
		
		for(Path file : localFiles) {
			DataInputStream d = new DataInputStream(fs.open(file));
			BufferedReader reader = new BufferedReader(new InputStreamReader(d));
			while ((line = reader.readLine()) != null){
				HashMap<Integer, Float> features = new HashMap<Integer, Float>();
				String[] stringFeatures = line.split(Constants.TEXT_SEPARATOR);
				for(String stringFeature : stringFeatures) {
					features.put(Integer.parseInt(stringFeature.trim()), (float) 1);
				}
				clusters.add(features);
			}
			reader.close();
		}
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		ArrayList<FeatureWritable> features = new ArrayList<FeatureWritable>();
		
		// Transform value into text and load into movie writable arraylist
		
		float maxSimilarity = 0;
		int optimalCluster = 0;
		
		for(int i = 0; i < clusters.size(); i++) {
			HashMap<Integer, Float> centroidFeatureVector = clusters.get(i);
			
			float centroidSize = getVectorSize(centroidFeatureVector);
			
			float featureVectorSize = 0;
			
			float cosineSimilarity = 0;
			
			for(FeatureWritable feature : features) {
				if(feature.getFeatureNumber() > centroidFeatureVector.size())
					new Exception("Feature is outside of centroid Feature vector");
				
				if(centroidFeatureVector.get(feature.getFeatureNumber()) != null) {
					float vectorVal = feature.getFeatureValue();
					float centroidVal = centroidFeatureVector.get(feature.getFeatureNumber());
				
					cosineSimilarity += vectorVal * centroidVal;
					
					featureVectorSize += Math.pow(vectorVal, 2);
				}
			}
			featureVectorSize = (float) Math.sqrt(featureVectorSize);
			
			cosineSimilarity = cosineSimilarity / (featureVectorSize * centroidSize);
			
			if(cosineSimilarity > maxSimilarity) {
				maxSimilarity = cosineSimilarity;
				optimalCluster = i;
			}
		}
		
		for(FeatureWritable feature : features) {
			outputKey.set(optimalCluster);
			outputValue = feature;
			context.write(outputKey, outputValue);
		}
	}
	
	private float getVectorSize(HashMap<Integer, Float> v) {
		float result = 0;
		
		for(Map.Entry<Integer, Float> feature : v.entrySet()) {
			result += Math.pow(feature.getValue(), 2);
		}
		
		return (float) Math.sqrt(result);
	}
}
