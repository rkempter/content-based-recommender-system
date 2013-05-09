package ch.epfl.advdb.milestone2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Class needed to determine the average radius of a cluster group.
 * Based on the average radius, we determine the optimal number of clusters needed.
 * 
 * @author rkempter
 */

public class AvgClusterRadius {

	@SuppressWarnings("deprecation")
	AvgClusterRadius(int clusterType, int nbrClusters, Configuration conf, String inputPath, String centroidPath, String outputPath) throws FileNotFoundException, IOException {	
		ArrayList<Hashtable<Integer, Float>> centroidList = new ArrayList<Hashtable<Integer, Float>>();
		float[] clusterRadius = new float[nbrClusters];
		
		FileSystem fs = FileSystem.get(conf);
		
		centroidList = readCentroids(conf, centroidPath);
		
		// for each file in the IMDB dataset
		FileStatus[] fileStatus = fs.listStatus(new Path(inputPath));
		for(FileStatus status : fileStatus) {
			
			String line;
			
			FSDataInputStream in = fs.open(status.getPath());

			while((line = in.readLine()) != null) {
				String[] stringFeatures = line.trim().split(Constants.TEXT_SEPARATOR);
				
				int optimalCluster;
				if(clusterType == Constants.IMDB_CLUSTER)
					optimalCluster = getOptimalCluster(stringFeatures, centroidList);
				else {
					optimalCluster = getOptimalCluster(line, centroidList);
					if(optimalCluster == -1)
						continue;
				}
				
				float radius = getDistance(centroidList.get(optimalCluster), stringFeatures);
				if(radius > clusterRadius[optimalCluster]) {
					clusterRadius[optimalCluster] = radius;
				}
			}
			
			in.close();	
		}
		
		float avgRadius = 0;
		for(int i = 0; i < nbrClusters; i++) {
			avgRadius += clusterRadius[i];
		}
		avgRadius = avgRadius / nbrClusters;
		
		writeRadiusToFile(fs, outputPath, nbrClusters, avgRadius);
	}
	
	private int getOptimalCluster(String[] stringFeatures, ArrayList<Hashtable<Integer, Float>> centroidList) {
		float vectorSize = getVectorSizeFromStrings(stringFeatures);
		
		int optimalCluster = 0;
		
		float maxCosineSimilarity = 0;
		
		for(int i = 0; i < centroidList.size(); i++) {
			Hashtable<Integer, Float> centroid = centroidList.get(i);
			float size = getFloatVectorSize(centroid);
			
			float cosineSimilarity = 0;
			
			// 0 is equal to movie id
			for(int j = 1; j < stringFeatures.length; j++) {
				int feature = Integer.parseInt(stringFeatures[j]);
				
				if(centroid.containsKey(feature)) {
					cosineSimilarity += centroid.get(feature) * 1;
				}
			}
			
			cosineSimilarity = cosineSimilarity / (size * vectorSize);
			
			if(maxCosineSimilarity < cosineSimilarity) {
				maxCosineSimilarity = cosineSimilarity;
				optimalCluster = i;
			}
		}
		
		return optimalCluster;
	}
	
	private int getOptimalCluster(String line, ArrayList<Hashtable<Integer, Float>> centroidList) {

		ArrayList<FeatureWritable> features = new ArrayList<FeatureWritable>();
		
		String[] movie = line.trim().split(Constants.TEXT_SEPARATOR);
		
		if(movie[0].equals("V")) {
			for(int i = 1; i < movie.length; i++) {
				features.add(new FeatureWritable(Integer.parseInt(movie[2]), Integer.parseInt(movie[1]), Float.parseFloat(movie[3])));
			}
		
			float maxSimilarity = 0;
			int optimalCluster = 0;
			
			for(int i = 0; i < centroidList.size(); i++) {
				Hashtable<Integer, Float> centroidFeatureVector = centroidList.get(i);
				
				float centroidSize = getFloatVectorSize(centroidFeatureVector);
				
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
			
			return optimalCluster;
		}
		
		return -1;
	}

	
	private ArrayList<Hashtable<Integer, Float>> readCentroids(Configuration conf,
			String centroidPath) throws FileNotFoundException, IOException {
		
		FileSystem fs = FileSystem.get(conf);
		
		ArrayList<Hashtable<Integer, Float>> centroidList = new ArrayList<Hashtable<Integer, Float>>();
		
		FileStatus[] fileStatus = fs.listStatus(new Path(centroidPath));
		
		for (FileStatus status : fileStatus) {
			Path localPath = status.getPath();
			String line;
			
			FSDataInputStream in = fs.open(localPath);
			while ((line = in.readLine()) != null) {
				
				String[] stringFeatures = line.trim().split(Constants.TEXT_SEPARATOR);
				int cluster = Integer.parseInt(stringFeatures[1]);
				while(centroidList.size() < cluster+1) {
					centroidList.add(new Hashtable<Integer, Float>());
				}
				centroidList.get(cluster).put(Integer.parseInt(stringFeatures[2]), Float.parseFloat(stringFeatures[3]));
			}
			
			in.close();
		}
		
		return centroidList;
	}

	private void writeRadiusToFile(FileSystem fs, String outputPath, int nbrClusters,
			float avgRadius) throws IOException {
		
		FSDataOutputStream out = fs.append(new Path(outputPath));
		String line = nbrClusters + Constants.TEXT_SEPARATOR + avgRadius + "\n";
		byte[] lineBytes = line.getBytes();
		out.write(lineBytes);
		out.close();
	}

	private float getFloatVectorSize(Hashtable<Integer, Float> point) {
		float size = 0;
		for(Map.Entry<Integer, Float> entry : point.entrySet()) {
			size += Math.pow(entry.getValue(), 2);
		}
		
		return (float) Math.sqrt(size);
	}

	private float getDistance(Hashtable<Integer, Float> hashtable,
			String[] stringFeatures) {
		
		int length = 0;
		
		for(String stringFeature : stringFeatures) {
			int feature = Integer.parseInt(stringFeature);
			if(hashtable.contains(feature)) {
				length += Math.pow((1.0 - hashtable.get(feature)), 2);
			} else {
				length += 1.0;
			}
		}
		
		return (float) Math.sqrt(length);
	}

	private float getVectorSizeFromStrings(String[] stringFeatures) {
		return (float) Math.sqrt(stringFeatures.length - 1);
	}
}
