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

public class ClusterMapper extends Mapper<LongWritable, Text, IntWritable, FeatureWritable>{

	private Path[] localFiles;
	
	private ArrayList<HashMap<Integer, Float>> clusters = new ArrayList<HashMap<Integer, Float>>();
	
	private IntWritable outputKey = new IntWritable();
	private FeatureWritable outputValue = new FeatureWritable();
	
	@SuppressWarnings("deprecation")
	public void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		
		localFiles = DistributedCache.getLocalCacheFiles(conf);
		
		String line = null;
		
		for(Path file : localFiles) {
			System.out.println("Path: "+file.toString());
			
			BufferedReader reader = new BufferedReader(new FileReader(file.toString()));
			while ((line = reader.readLine()) != null){
				System.out.println("Line: "+line);
				HashMap<Integer, Float> features = new HashMap<Integer, Float>();
				String[] stringFeatures = line.trim().split(Constants.TEXT_SEPARATOR);
				for(String stringFeature : stringFeatures) {
					if(stringFeature == "") break; 
					int feature = Integer.parseInt(stringFeature.trim());
					features.put(feature, (float) 1);
				}
				
				clusters.add(features);
			}
			reader.close();
		}
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		ArrayList<FeatureWritable> features = new ArrayList<FeatureWritable>();
		
		String[] movie = value.toString().trim().split(Constants.TEXT_SEPARATOR);
		for(int i = 1; i < movie.length; i++) {
			features.add(new FeatureWritable(Integer.parseInt(movie[0]), Integer.parseInt(movie[i]), (float) 1));
		}
		
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
			
			cosineSimilarity = (featureVectorSize == 0 || centroidSize == 0) ? 0 : cosineSimilarity / (featureVectorSize * centroidSize);
			
			if(cosineSimilarity > maxSimilarity) {
				maxSimilarity = cosineSimilarity;
				optimalCluster = i;
			}
		}
		
		System.out.println("Cosine Similarity: "+maxSimilarity);
		
		for(FeatureWritable feature : features) {
			System.out.println("Optimal cluster: "+optimalCluster);
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
