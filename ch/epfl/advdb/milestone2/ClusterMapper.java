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
	
	protected HashMap<Integer, Float>[] clusters;
	
	protected int numberOfClusters;
	protected int clusterType;
	
	protected IntWritable outputKey = new IntWritable();
	protected FeatureWritable outputValue = new FeatureWritable();
	
	@SuppressWarnings({ "deprecation", "unchecked" })
	public void setup(Context context) throws IOException {
		Configuration conf = context.getConfiguration();
		
		numberOfClusters = conf.getInt(Constants.FEATURE_DIMENSION_STRING, 10);
		clusterType = conf.getInt(Constants.MATRIX_TYPE, Constants.NETFLIX_CLUSTER);
		
		clusters = new HashMap[numberOfClusters];
		
		localFiles = DistributedCache.getLocalCacheFiles(conf);
		
		String line = null;
		
		int lastClusterNumber = -1;
		boolean init = false;
		HashMap<Integer, Float> features = new HashMap<Integer, Float>();
		
		for(Path file : localFiles) {
			
			BufferedReader reader = new BufferedReader(new FileReader(file.toString()));
			while ((line = reader.readLine()) != null){
				
				String[] stringFeatures = line.trim().split(Constants.TEXT_SEPARATOR);
				
				if(!init) {
					lastClusterNumber = Integer.parseInt(stringFeatures[1]);
					init = true;
				}
				
				if(stringFeatures.length != 4)
					new Exception("Problem with reading the file. Not correct format!");
				
				if(lastClusterNumber != Integer.parseInt(stringFeatures[1])) {
					clusters[lastClusterNumber] = features;
					lastClusterNumber = Integer.parseInt(stringFeatures[1]);
					features = new HashMap<Integer, Float>();
				}
				
				features.put(Integer.parseInt(stringFeatures[2]), Float.parseFloat(stringFeatures[3]));
			}
			reader.close();
		}
		clusters[lastClusterNumber] = features;
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
				features.add(new FeatureWritable(Integer.parseInt(movie[0]), i, Float.parseFloat(movie[i])));
			}
		}
		
		float maxSimilarity = 0;
		int optimalCluster = 0;
		
		for(int i = 0; i < clusters.length; i++) {
			HashMap<Integer, Float> centroidFeatureVector = clusters[i];
			
			if(centroidFeatureVector == null) {
				centroidFeatureVector = new HashMap<Integer, Float>();
			}
			
			float centroidSize = getVectorSize(centroidFeatureVector);
			
			float featureVectorSize = 0;
			
			float cosineSimilarity = 0;
			
			for(FeatureWritable feature : features) {
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
		
		for(FeatureWritable feature : features) {
			outputKey.set(optimalCluster);
			outputValue = feature;
			context.write(outputKey, outputValue);
		}
	}
	
	protected float getVectorSize(HashMap<Integer, Float> v) {
		float result = 0;
		
		for(Map.Entry<Integer, Float> feature : v.entrySet()) {
			result += Math.pow(feature.getValue(), 2);
		}
		
		return (float) Math.sqrt(result);
	}
}
