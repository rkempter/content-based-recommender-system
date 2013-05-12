package ch.epfl.advdb.milestone2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class NetflixCentroidBuilder {
	
	Configuration configuration = null;
	String inputDir = null;
	String outputDir = null;
	int numOfNetflixClusters;
	
	public NetflixCentroidBuilder(Configuration conf, String inputDir, String outputDir, int numOfNetflixClusters) {
		this.configuration = conf;
		this.inputDir = inputDir;
		this.outputDir = outputDir;
		this.numOfNetflixClusters = numOfNetflixClusters;
	}
	
	public void createInitialAdvancedCentroids() throws IOException {
		ArrayList<float[]> centroids = new ArrayList<float[]>();
		
		centroids.add(getFirstCentroid());
		
		for(int i = 1; i < numOfNetflixClusters; i++) {
			centroids.add(new float[10]);
			String line;
			float[] features = new float[10];
			float distance = 0;
			float maxDistance = 0;
			int column = 0;
			
			FileSystem fs = FileSystem.get(configuration);
			FileStatus[] status = fs.listStatus(new Path(inputDir));
			
			for(int j = 0; j < status.length; j++) {
			
				BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[j].getPath())));
				
				while((line = br.readLine()) != null) {
					String[] element = line.split(Constants.TEXT_SEPARATOR);
					
					if(element[0].equals("E")) {
						float minDistance = 1000000;
						for(float[] centroid : centroids) {
							distance = computeDistance(features, centroid);
							if(minDistance > distance) {
								minDistance = distance;
							}
						}
						if(minDistance > maxDistance) {
							maxDistance = minDistance;
							centroids.set(i, features);
						}
						features = new float[10];
					} else {
						// V, row, column, value
						column = Integer.parseInt(element[2]);
						int row = Integer.parseInt(element[1]) - 1;
						features[row] = Float.parseFloat(element[3]);
					}
				}
			}
		}
		
		writeCentroidToHDFS(centroids);
	}
	
	private float[] getFirstCentroid() throws IOException {
		Random r = new Random();
		String line;
		FileSystem fs = FileSystem.get(configuration);
		FileStatus[] status = fs.listStatus(new Path(inputDir));
		
		boolean read = true;
		
		float[] features = new float[10];
		int lineCounter = 0;
		
		for(int j = 0; j < status.length; j++) {
		
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[j].getPath())));
			
			while((line = br.readLine()) != null) {
				String[] element = line.split(Constants.TEXT_SEPARATOR);
				
				if(element[0].equals("E")) {
					lineCounter++;
					read = false;
					if(r.nextInt(lineCounter) < 1) {
						read = true;
					}
				} else if(read){
					int row = Integer.parseInt(element[1]) - 1;
					features[row] = Float.parseFloat(element[3]);
				}
			}
		}
		
		return features;
	}
	
	private float computeDistance(float[] features, float[] centroid) {
			
		float length = 0;
			
		for(int i = 0; i < features.length; i++) {
			length += Math.pow((features[i] - centroid[i]), 2);
		}
			
		return (float) Math.sqrt(length);
	}

	private void writeCentroidToHDFS(ArrayList<float[]> centroids) throws IOException {
		FileSystem fs = FileSystem.get(configuration);
		
		FSDataOutputStream out = fs.create(new Path(outputDir));
		for(int i = 0; i < centroids.size(); i++) {
			float[] features = centroids.get(i);
			for(int feat = 0; feat < features.length; feat++) {
				String line = "N";
				line += Constants.TEXT_SEPARATOR + i + Constants.TEXT_SEPARATOR + feat + Constants.TEXT_SEPARATOR + features[feat] + "\n";
				byte[] stringInBytes = line.getBytes();
				out.write(stringInBytes);
			}
		}
		
		out.close();
		fs.close();
	}
}
