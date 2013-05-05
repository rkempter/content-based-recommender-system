package ch.epfl.advdb.milestone2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
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

	public void createInitialCentroids() throws IOException {
		
		ArrayList<Float[]> centroids = new ArrayList<Float[]>();
		Random random = new Random();
		
		FileSystem fs = FileSystem.get(configuration);
		FileStatus[] status = fs.listStatus(new Path(inputDir));
		
		int lineCounter = 0;
		int columnCounter = -2;
		
		for(int i = 0; i < status.length; i++) {
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
			String line = br.readLine();

			boolean read = false;
			boolean init = false;
			int r = 0;
			Float[] values = null;

			while(line != null) {
				
				if(lineCounter % 11 == 0) {
					// Add values to centroid (if we just read one)
					columnCounter++;
					if(init && read) {
						if(columnCounter < numOfNetflixClusters)
							centroids.add(values);
						else
							centroids.set(r, values);
					}
					// Check if next line should be read (Reservoir Sampling)
					values = new Float[10];
					r = columnCounter < numOfNetflixClusters ? columnCounter : random.nextInt(columnCounter);
					
					read = false;
					if(r < numOfNetflixClusters) {
						read = true;
					}
					init = true;
				}
				
				if(read && lineCounter % 11 != 10) {
					values = readFloatFromLine(line, values);
				}
				
				line = br.readLine();
				lineCounter++;
				
			}
		}
		
		writeCentroidToHDFS(centroids);
	}
	
	
	/**
	 * Read float value from line
	 * 
	 * @param line
	 * @param values
	 * @return
	 */
	private Float[] readFloatFromLine(String line, Float[] values) {
		
		String[] element = line.split(Constants.TEXT_SEPARATOR);
		
		if(element.length > 0) {
			float value = Float.parseFloat(element[3].trim());
			int row = Integer.parseInt(element[1].trim()) - 1;
			values[row] = value;
		}
		
		return values;
	}

	private void writeCentroidToHDFS(ArrayList<Float[]> centroids) throws IOException {
		FileSystem fs = FileSystem.get(configuration);
		
		FSDataOutputStream out = fs.create(new Path(outputDir));
		for(int i = 0; i < centroids.size(); i++) {
			Float[] features = centroids.get(i);
			String line = "";
			line = features[0].toString();
			for(int feat = 1; feat < features.length; feat++) {
				line += Constants.TEXT_SEPARATOR + features[feat].toString();
			}
			line += "\n";
			
			byte[] stringInBytes = line.getBytes();
			out.write(stringInBytes);
		}
		
		out.close();
		fs.close();
	}
}
