package ch.epfl.advdb.milestone2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class IMDBCentroidBuilder {
	
	Configuration configuration = null;
	String inputDir = null;
	String outputDir = null;
	int numOfIMDBClusters;
	
	public IMDBCentroidBuilder(Configuration conf, String inputDir, String outputDir, int numOfIMDBClusters) {
		this.configuration = conf;
		this.inputDir = inputDir;
		this.outputDir = outputDir;
		this.numOfIMDBClusters = numOfIMDBClusters;
	}
	
	/**
	 * Extension to k-means++
	 * @throws IOException
	 */
	public void createInitialAdvancedCentroids() throws IOException {
		ArrayList<ArrayList<Integer>> centroids = new ArrayList<ArrayList<Integer>>();
		
		FileSystem fs = FileSystem.get(configuration);
		FileStatus[] status = fs.listStatus(new Path(inputDir));
		
		ArrayList<Integer> firstCentroid = getFirstRandomCentroid();
		centroids.add(firstCentroid);
		
		float minDistance = 0;
		
		for(int clust = 1; clust < numOfIMDBClusters; clust++) {
			// initialize array with clust float values
			float maxDistance = 0;
			centroids.add(new ArrayList<Integer>());
			
			for(int i = 0; i < status.length; i++) {
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
				String line;
				
				while((line = br.readLine()) != null) {
					float distance = 0;
					
					ArrayList<Integer> newFeatures = getIMDBFeaturesFromLine(line);
					if(newFeatures.size() == 0)
						continue;
					
					minDistance = 100000;
					for(ArrayList<Integer> centroid : centroids) {
						distance = getDistance(centroid, newFeatures);
						if(distance < minDistance) {
							minDistance = distance;
						}
					}
					
					if(minDistance > maxDistance) {
						maxDistance = minDistance;
						centroids.set(clust, newFeatures);
					}
				}
			}
		
		}
		
		writeCentroidToHDFS(centroids);
	}
	
	private float getDistance(ArrayList<Integer> centroid, ArrayList<Integer> newFeatures) {
		HashMap<Integer, Boolean> lookup = new HashMap<Integer, Boolean>();
		
		int cSize = centroid.size();
		int fSize = newFeatures.size();
		
		
		for(Integer i : centroid) {
			lookup.put(i, true);
		}
		
		for(Integer f : newFeatures) {
			if(lookup.containsKey(f)) {
				cSize--;
				fSize--;
			}
		}
		
		return (float) Math.sqrt(cSize+fSize);
	}
	
	
	private ArrayList<Integer> getFirstRandomCentroid() throws IOException {
		ArrayList<Integer> firstCentroid = new ArrayList<Integer>();
		Random random = new Random();
		
		FileSystem fs = FileSystem.get(configuration);
		FileStatus[] status = fs.listStatus(new Path(inputDir));
		
		int lineCounter = 0;
		
		for(int i = 0; i < status.length; i++) {
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
			String line;
			
			while((line = br.readLine()) != null) {
				lineCounter++;
				int r = random.nextInt(lineCounter);
				if(r < 1) {
					ArrayList<Integer> features = getIMDBFeaturesFromLine(line);
					if(features.size() == 0)
						continue;
					firstCentroid = features;
				}
			}
		}
		
		return firstCentroid;
	}
	
	/**
	 * Transforms a line into a list of features
	 * @param line
	 * @return
	 */
	private ArrayList<Integer> getIMDBFeaturesFromLine(String line) {
		ArrayList<Integer> features = new ArrayList<Integer>();
		
		String[] stringFeatures = line.split(Constants.TEXT_SEPARATOR);
		
		int i = 0;
		for(String stringFeature : stringFeatures) {
			// we dont want the first element (movie id)
			if(i != 0) {
				features.add(Integer.parseInt(stringFeature.trim()));
			} else {
				i++;
			}
		}
		
		return features;
	}

	private void writeCentroidToHDFS(ArrayList<ArrayList<Integer>> centroids) throws IOException {
		
		// Write out in correct format.
		FileSystem fs = FileSystem.get(configuration);
		
		FSDataOutputStream out = fs.create(new Path(outputDir));
		for(int i = 0; i < centroids.size(); i++) {
			ArrayList<Integer> features = centroids.get(i);
			if(features.size() > 0) {
				for(int feat = 0; feat < features.size(); feat++) {
					String line = "I";
					line += Constants.TEXT_SEPARATOR + i + Constants.TEXT_SEPARATOR + features.get(feat).toString() + Constants.TEXT_SEPARATOR + 1 + "\n";
					byte[] stringInBytes = line.getBytes();
					out.write(stringInBytes);
				}
			}
		}
		
		out.close();
		fs.close();
	}
}
