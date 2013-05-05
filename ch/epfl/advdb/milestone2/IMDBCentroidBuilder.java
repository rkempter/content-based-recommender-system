package ch.epfl.advdb.milestone2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
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
	 * Reservoir sampling
	 * 
	 * @param nbr
	 * @param conf
	 * @throws IOException
	 */
	public void createInitialCentroids() throws IOException {
		
		ArrayList<ArrayList<Integer>> centroids = new ArrayList<ArrayList<Integer>>();
		Random random = new Random();
		
		FileSystem fs = FileSystem.get(configuration);
		FileStatus[] status = fs.listStatus(new Path(inputDir));
		
		int lineCounter = 0;
		
		for(int i = 0; i < status.length; i++) {
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
			String line = br.readLine();
			
			while(line != null) {
				if(lineCounter < numOfIMDBClusters) {
					ArrayList<Integer> features = getIMDBFeaturesFromLine(line);
					centroids.add(features);
				} else {
					int r = random.nextInt(lineCounter);
					if(r < numOfIMDBClusters) {
						ArrayList<Integer> features = getIMDBFeaturesFromLine(line);
						centroids.set(r, features);
					}
				}
				
				line = br.readLine();
				lineCounter++;
			}
		}
		
		writeCentroidToHDFS(centroids);
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
			
			String line = new String();
			if(features.size() > 0) {
				line = features.get(0).toString();
				for(int feat = 1; feat < features.size(); feat++) {
					line += Constants.TEXT_SEPARATOR + features.get(feat).toString();
				}
			}
			line += "\n";
			byte[] stringInBytes = line.getBytes();
			out.write(stringInBytes);
		}
		
		out.close();
		fs.close();
	}
}
