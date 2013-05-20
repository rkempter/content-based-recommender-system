package ch.epfl.advdb.milestone2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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
		
		numberOfClusters = conf.getInt(Constants.NBR_CLUSTERS, 10);
		clusterType = conf.getInt(Constants.MATRIX_TYPE, Constants.NETFLIX_CLUSTER);
		
		localFiles = DistributedCache.getLocalCacheFiles(conf);
		
		clusters = Utility.readCluster(new HashMap[numberOfClusters], localFiles);
	}
	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		ArrayList<FeatureWritable> features = new ArrayList<FeatureWritable>();
		
		String[] movie = value.toString().trim().split(Constants.TEXT_SEPARATOR);
		
		// Same class for both netflix and imdb dataset.
		if(clusterType == Constants.IMDB_CLUSTER) {
			for(int i = 1; i < movie.length; i++) {
				features.add(new FeatureWritable(Integer.parseInt(movie[0]), Integer.parseInt(movie[i]), (float) 1));
			}
		} else {
			float size = 0;
			// Spherical k-means - normalize the vectors
			for(int i = 1; i < movie.length; i++) {
				size += Math.pow(Float.parseFloat(movie[i].trim()), 2);
			}
			size = (float) Math.sqrt(size);
			for(int i = 1; i < movie.length; i++) {
				features.add(new FeatureWritable(Integer.parseInt(movie[0]), i-1, Float.parseFloat(movie[i].trim()) / size));
			}
		}
		
		// Find optimal cluster
		int optimalCluster = Utility.getBestCluster(clusters, features);
		
		// Send each fature and its corresponding value with the cluster-id to the reducer
		for(FeatureWritable feature : features) {
			outputKey.set(optimalCluster);
			outputValue = feature;
			context.write(outputKey, outputValue);
		}
	}
}
