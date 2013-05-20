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
	ArrayList<float[]> allMovies = new ArrayList<float[]>();
	
	/**
	 * Constructor
	 * 
	 * @param conf
	 * @param inputDir
	 * @param outputDir
	 * @param numOfNetflixClusters
	 */
	public NetflixCentroidBuilder(Configuration conf, String inputDir, String outputDir, int numOfNetflixClusters) {
		this.configuration = conf;
		this.inputDir = inputDir;
		this.outputDir = outputDir;
		this.numOfNetflixClusters = numOfNetflixClusters;
	}
	
	/**
	 * Selects randomly the first centroid and runs over all
	 * of them numOfNetflixClusters-1 times to select everytime
	 * the one with the largest minimum distance to all others
	 * already selected centroids.
	 * 
	 * @throws IOException
	 */
	public void createInitialAdvancedCentroids() throws IOException {
		readIntoMemory();
		
		ArrayList<float[]> centroids = new ArrayList<float[]>();
		Random r = new Random();
		int first = r.nextInt(allMovies.size());
		float[] features;
		
		centroids.add(allMovies.get(first));
		
		for(int i = 1; i < numOfNetflixClusters; i++) {
			centroids.add(new float[10]);
			float distance = 0;
			float maxDistance = 0;
			
			for(int j = 0; j < allMovies.size(); j++) {
				features = allMovies.get(j);
				float minDistance = 100000000;
				
				for(float[] centroid : centroids) {
					distance = computeDistance(features, centroid);
					if(minDistance > distance) {
						minDistance = distance;
					}
				}
				
				if(minDistance >= maxDistance) {
					maxDistance = minDistance;
					centroids.set(i, features);
				}
			}
		}
		
		writeCentroidToHDFS(centroids);
	}
	
	/**
	 * Reads the netflix matrix into memory (possible because of the size)
	 * 
	 */
	private void readIntoMemory() throws IOException {
		FileSystem fs = FileSystem.get(configuration);
		FileStatus[] status = fs.listStatus(new Path(inputDir));
		String line;
		float[] features = new float[10];
		
		for(int j = 0; j < status.length; j++) {
		
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[j].getPath())));
			
			while((line = br.readLine()) != null) {
				String[] element = line.split(Constants.TEXT_SEPARATOR);
				
				if(element[0].equals("E")) {
					float size = Utility.getVectorSize(features);
					for(int i = 0; i < features.length; i++) {
						features[i] = features[i] / size;
					}
					allMovies.add(features);
					features = new float[10];
				} else {
					int row = Integer.parseInt(element[1]) - 1;
					features[row] = Float.parseFloat(element[3]);
				}
			}
		}
	}
	
	private float computeDistance(float[] features, float[] centroid) {
			
		float length = 0;
			
		for(int i = 0; i < features.length; i++) {
			length += Math.pow((features[i] - centroid[i]), 2);
		}
			
		return (float) Math.sqrt(length);
	}

	/**
	 * Write the centroid to hdfs
	 * 
	 * @param ArrayList<float[]> centroids
	 * @throws IOException
	 */
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
