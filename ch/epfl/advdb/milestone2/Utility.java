package ch.epfl.advdb.milestone2;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

public class Utility {
	
	public Utility() {
		
	}

	/**
	 * Reads a file with line format %c, %d, %d, %f (type, movieId, featureId, value)
	 * @param HashMap[] clusters
	 * @param Path[] localFiles
	 * @return HashMap<Integer, Float>[]
	 * @throws IOException 
	 * @throws NumberFormatException 
	 */
	public static HashMap<Integer, Float>[] readCluster(HashMap<Integer, Float>[] clusters, Path[] localFiles) throws NumberFormatException, IOException {
		
		String line = null;
			
		for(Path file : localFiles) {
			// Check if file contains word centroid
			if(!file.toString().matches(".*(_centroids_).*"))
				continue;
				
			BufferedReader reader = new BufferedReader(new FileReader(file.toString()));
			while ((line = reader.readLine()) != null){
				String[] stringFeatures = line.trim().split(Constants.TEXT_SEPARATOR);
					
				int clusterId = Integer.parseInt(stringFeatures[1]);
				
				if(clusters[clusterId] == null) {
					clusters[clusterId] = new HashMap<Integer, Float>();
				}
				clusters[clusterId].put(Integer.parseInt(stringFeatures[2]), Float.parseFloat(stringFeatures[3]));
			}
			reader.close();
		}
		
		return clusters;
	}
	
	/**
	 * Computes the best cluster for a vector
	 * 
	 * @param HashMap<Integer, Float>[] clusters
	 * @param ArrayList<FeatureWritable> vector
	 * @return int
	 */
	public static int getBestCluster(HashMap<Integer, Float>[] clusters, ArrayList<FeatureWritable> features) {
		float maxSimilarity = 0;
		int optimalCluster = 0;
		float cosineSimilarity = 0;
		
		for(int i = 0; i < clusters.length; i++) {
			
			cosineSimilarity = getCosineSimilarity(clusters[i], features);
			
			if(cosineSimilarity > maxSimilarity) {
				maxSimilarity = cosineSimilarity;
				optimalCluster = i;
			}
		}
		
		return optimalCluster;
	}
	
	/**
	 * Computes the cosine similarity between a HashMap<Integer, Float> vector and 
	 * an FeatureWritable-ArrayList vector
	 * 
	 * @param HashMap<Integer, Float> centroid
	 * @param ArrayList<FeatureWritable> vector
	 * @return float
	 */
	public static float getCosineSimilarity(HashMap<Integer, Float> centroid, ArrayList<FeatureWritable> vector) {
		float cosineSimilarity = 0;
		float centroidSize = getVectorSize(centroid);
		float featureVectorSize = getVectorSize(vector);
		
		if(featureVectorSize == 0 || centroidSize == 0)
			return 0;
		
		for(FeatureWritable feature : vector) {
			if(centroid.get(feature.getFeatureNumber()) != null) {
				float vectorVal = feature.getFeatureValue();
				float centroidVal = centroid.get(feature.getFeatureNumber());
			
				cosineSimilarity += vectorVal * centroidVal;
			}
		}
		
		return cosineSimilarity / (featureVectorSize * centroidSize);
	}
	

	/**
	 * Computes the length of an FeatureWritable ArrayList-vector
	 * 
	 * @param ArrayList<FeatureWritable> vector
	 * @return float
	 */
	public static float getVectorSize(ArrayList<FeatureWritable> vector) {
		float length = 0;
		
		for(FeatureWritable el : vector) {
			length += Math.pow(el.getFeatureValue(), 2);
		}
		
		return (float) Math.sqrt(length);
	}
	
	/**
	 * Computes the length of an HashMap-vector
	 * 
	 * @param ArrayList<FeatureWritable> vector
	 * @return float
	 */
	public static float getVectorSize(HashMap<Integer, Float> vector) {
		float length = 0;
		for(Map.Entry<Integer, Float> el : vector.entrySet()) {
			length += Math.pow(el.getValue(), 2);
		}
		
		return (float) Math.sqrt(length);
	}
	
	/**
	 * Computes the length of an HashMap-vector
	 * 
	 * @param ArrayList<FeatureWritable> vector
	 * @return float
	 */
	public static float getVectorSize(float[] v) {
		float length = 0;
		for(int i = 0; i < v.length; i++) {
			length += Math.pow(v[i], 2);
		}
		
		return (float) Math.sqrt(length);
	}

	public static float[] readUValues(String[] elements) {
		float[] uValues = new float[10];
		
		for(int i = 1; i < 11; i++) {
			uValues[i-1] = Float.parseFloat(elements[i]);
		}
		
		return uValues;
	}
	
	public static float getUserRating(float[] v, float[] u) {
		float sum = 0;
		for(int i = 0; i < Constants.NUM_OF_NETFLIX_FEATURES; i++) {
			sum += v[i] * u[i];
		}
		
		return sum;
	}
}
