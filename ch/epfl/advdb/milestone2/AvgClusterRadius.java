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
	
	private int clusterType;
	private int nbrClusters;
	private Configuration conf;
	String inputPath;
	String centroidPath;
	String outputPath;
	FileSystem fs;
	ArrayList<HashMap<Integer, Float>> centroidList = new ArrayList<HashMap<Integer, Float>>();

	@SuppressWarnings("deprecation")
	AvgClusterRadius(int clusterType, int nbrClusters, Configuration conf, String inputPath, String centroidPath, String outputPath) throws FileNotFoundException, IOException {	
		
		this.clusterType = clusterType;
		this.nbrClusters = nbrClusters;
		this.conf = conf;
		this.inputPath = inputPath;
		this.centroidPath = centroidPath;
		this.outputPath = outputPath;
		this.fs = FileSystem.get(conf);
		
		centroidList = readCentroids(conf, centroidPath);
	}
	
	public float execute() throws FileNotFoundException, IOException {
		if(clusterType == Constants.IMDB_CLUSTER)
			return executeI();
		else
			return executeV();
	}
	
	private float executeI() throws FileNotFoundException, IOException {
		float[] clusterRadius = new float[nbrClusters];
		
		FileStatus[] fileStatus = fs.listStatus(new Path(inputPath));
		for(FileStatus status : fileStatus) {
			
			String line;
			
			FSDataInputStream in = fs.open(status.getPath());

			while((line = in.readLine()) != null) {
				String[] stringFeatures = line.trim().split(Constants.TEXT_SEPARATOR);
				
				int optimalCluster = getOptimalCluster(stringFeatures);
				
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
		return avgRadius / nbrClusters;
	}
	
	
	public float executeV() throws IOException {
		
		ArrayList<float[]> movies = new ArrayList<float[]>();
		float[] clusterRadius = new float[nbrClusters];
		
		// for each file in the IMDB dataset
		FileStatus[] fileStatus = fs.listStatus(new Path(inputPath));
		for(FileStatus status : fileStatus) {
					
			String line;
					
			FSDataInputStream in = fs.open(status.getPath());
			int i = 0;
			
			movies.add(new float[10]);
			
			while((line = in.readLine()) != null) {
				
				String[] stringFeatures = line.trim().split(Constants.TEXT_SEPARATOR);
				
				if(stringFeatures[0].equals("E"))
					continue;
				
				i++;
				int movieId = Integer.parseInt(stringFeatures[2]) - 1;
				int row = Integer.parseInt(stringFeatures[1]) - 1;
				
				while(movieId >= movies.size())
					movies.add(new float[10]);
				
				movies.get(movieId)[row] = Float.parseFloat(stringFeatures[3]);

				if(i % 10 == 0) {
					int optimalCluster = getOptimalCluster(movies.get(movieId));
					float radius = getDistance(centroidList.get(optimalCluster), movies.get(movieId));

					if(radius > clusterRadius[optimalCluster]) {
						clusterRadius[optimalCluster] = radius;
					}
				}
			}
						
			in.close();	
		}
		
		float avgRadius = 0;
		for(int i = 0; i < nbrClusters; i++) {
			avgRadius += clusterRadius[i];
		}
		return avgRadius / nbrClusters;
	}
	
	private int getOptimalCluster(String[] stringFeatures) {
		float vectorSize = getVectorSizeFromStrings(stringFeatures);
		
		int optimalCluster = 0;
		
		float maxCosineSimilarity = 0;
		
		for(int i = 0; i < centroidList.size(); i++) {
			HashMap<Integer, Float> centroid = centroidList.get(i);
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
	
	private int getOptimalCluster(float[] movie) {

		float vectorSize = 0;
		for(int j = 0; j < movie.length; j++) {
			vectorSize += Math.pow(movie[j], 2);
		}
		
		int optimalCluster = 0;
		
		float maxCosineSimilarity = 0;
		
		for(int i = 0; i < centroidList.size(); i++) {
			HashMap<Integer, Float> centroid = centroidList.get(i);
			
			System.out.println("centroid size: "+centroid.size());
			
			float size = getFloatVectorSize(centroid);
			
			float cosineSimilarity = 0;
			
			// 0 is equal to movie id
			for(int j = 0; j < movie.length; j++) {
				System.out.println("Element at j: "+centroid.get(j));
				cosineSimilarity += centroid.get(j+1) * movie[j];
			}
			
			cosineSimilarity = cosineSimilarity / (size * vectorSize);
			
			if(maxCosineSimilarity < cosineSimilarity) {
				maxCosineSimilarity = cosineSimilarity;
				optimalCluster = i;
			}
		}
		
		return optimalCluster;
	}

	
	private ArrayList<HashMap<Integer, Float>> readCentroids(Configuration conf,
			String centroidPath) throws FileNotFoundException, IOException {
		
		FileSystem fs = FileSystem.get(conf);
		
		ArrayList<HashMap<Integer, Float>> centroidList = new ArrayList<HashMap<Integer, Float>>();
		
		FileStatus[] fileStatus = fs.listStatus(new Path(centroidPath));
		
		for (FileStatus status : fileStatus) {
			Path localPath = status.getPath();
			String line;
			
			FSDataInputStream in = fs.open(localPath);
			while ((line = in.readLine()) != null) {
				
				String[] stringFeatures = line.trim().split(Constants.TEXT_SEPARATOR);
				int cluster = Integer.parseInt(stringFeatures[1]);
				while(centroidList.size() < cluster+1) {
					centroidList.add(new HashMap<Integer, Float>());
				}
				centroidList.get(cluster).put(Integer.parseInt(stringFeatures[2]), Float.parseFloat(stringFeatures[3]));
			}
			
			in.close();
		}
		
		return centroidList;
	}

	public void writeRadiusToFile(float avgRadius) throws IOException {
		
		FSDataOutputStream out = fs.append(new Path(outputPath));
		String line = nbrClusters + Constants.TEXT_SEPARATOR + avgRadius + "\n";
		byte[] lineBytes = line.getBytes();
		out.write(lineBytes);
		out.close();
	}

	private float getFloatVectorSize(HashMap<Integer, Float> point) {
		float size = 0;
		for(Map.Entry<Integer, Float> entry : point.entrySet()) {
			size += Math.pow(entry.getValue(), 2);
		}
		
		return (float) Math.sqrt(size);
	}

	public float getDistance(HashMap<Integer, Float> hashtable,
			String[] stringFeatures) {
		
		float length = 0;
		
		for(int i = 1; i < stringFeatures.length; i++) {
			int feature = Integer.parseInt(stringFeatures[i]);
			if(hashtable.containsKey(feature)) {
				length += Math.pow((1.0 - hashtable.get(feature)), 2);
			} else {
				length += 1.0;
			}
		}
		
		return (float) Math.sqrt(length);
	}
	
	public float getDistance(HashMap<Integer, Float> hashtable,
			float[] features) {
		
		float length = 0;
		
		for(int i = 0; i < features.length; i++) {
			length += Math.pow((features[i] - hashtable.get(i+1)), 2);
		}
		
		return (float) Math.sqrt(length);
	}

	private float getVectorSizeFromStrings(String[] stringFeatures) {
		return (float) Math.sqrt(stringFeatures.length - 1);
	}
}
