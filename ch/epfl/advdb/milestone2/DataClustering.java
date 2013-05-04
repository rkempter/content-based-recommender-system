package ch.epfl.advdb.milestone2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class DataClustering {
	
	Configuration configuration;
	
	String inputDir, outputDir;
	int numOfIterations, numOfReducers, numOfMappers, numOfIMDBClusters, numOfVClusters;
	
	public DataClustering(String inputDir, String outputDir,
						  int numOfIterations, int numOfReducers,
						  int numOfMappers, int numOfIMDBClusters,
						  int numOfVClusters) {
		
		this.inputDir = inputDir;
		this.outputDir = outputDir;
		this.numOfIterations = numOfIterations;
		this.numOfReducers = numOfReducers;
		this.numOfMappers = numOfMappers;
		this.numOfIMDBClusters = numOfIMDBClusters;
		this.numOfVClusters = numOfVClusters;
		
		configuration = new Configuration();
		configuration.set("mapred.textoutputformat.separator",
						  String.valueOf(Constants.TEXT_SEPARATOR));
		
		configuration.setInt("mapred.map.tasks", numOfMappers);
	}
	
	private String getName(String str, int iteration) {
		return str+"_"+iteration;
	}

	
	@SuppressWarnings("deprecation")
	public void execute() 
			throws IOException, InterruptedException, ClassNotFoundException {
		
		{
			// create first centroids
		}
		
		for(int iter = 0; iter < numOfIterations; iter++) {
			
			{
				Configuration conf = new Configuration(configuration);
				conf.setInt("mapred.reduce.tasks", numOfIMDBClusters);
				DistributedCache.addCacheFile(new Path(getName(outputDir+"/imdb/", iter)).toUri(), conf);
				
				DataClusteringJob imdbClustering = new DataClusteringJob(
						conf, getName("IMDB Clustering", iter),
						ClusterMapper.class, ClusterReducer.class,
						getName(outputDir+"/imdb", iter), inputDir+"/imdb/",
						getName(outputDir+"/imdb", (iter+1)));
				
				imdbClustering.waitForCompletion(true);
			}
			
			{
				Configuration conf = new Configuration(configuration);
				conf.setInt("mapred.reduce.tasks", numOfVClusters);
				DistributedCache.addCacheFile(new Path(getName(outputDir+"/netflix/", iter)).toUri(), conf);
				
				DataClusteringJob netflixClustering = new DataClusteringJob(conf,
					getName("Netflix Clustering", iter),
					ClusterMapper.class, ClusterReducer.class,
					getName(outputDir+"/netflix", iter), inputDir+"/netflix/",
					getName(outputDir+"/netflix", iter+1));
				
				netflixClustering.waitForCompletion(true);
			}
			
		}	
	}

	/**
	 * Reservoir sampling
	 * 
	 * @param nbr
	 * @param conf
	 * @throws IOException
	 */
	public void initialIMDBCentroids() throws IOException {
		
		ArrayList<ArrayList<Integer>> centroids = new ArrayList<ArrayList<Integer>>();
		Random random = new Random();
		
		FileSystem fs = FileSystem.get(configuration);
		FileStatus[] status = fs.listStatus(new Path(inputDir));
		
		for(int i = 0; i < status.length; i++) {
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(status[i].getPath())));
			String line = br.readLine();
			int r = random.nextInt(i);
			
			if(i < numOfIMDBClusters || r < numOfIMDBClusters) {
				ArrayList<Integer> features = getIMDBFeaturesFromLine(line);
				centroids.add(features);
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
		
		FSDataOutputStream out = fs.create(new Path(outputDir+"/centroids/centroids.txt"));
		for(int i = 0; i < centroids.size(); i++) {
			ArrayList<Integer> features = centroids.get(i);
			
			String line = features.get(0).toString();
			for(int feat = 1; feat < features.size(); feat++) {
				line += Constants.TEXT_SEPARATOR + features.get(feat).toString();
			}
			line += "\n";
			
			out.writeChars(line);
		}
		
		out.close();
		fs.close();
	}
	
	private class DataClusteringJob extends Job {
		
		@SuppressWarnings("deprecation")
		public DataClusteringJob(Configuration conf, String jobName,
				Class<? extends Mapper<LongWritable, Text, 
						IntWritable, FeatureWritable>> mapper,
				Class<? extends Reducer<IntWritable, FeatureWritable, 
						CentroidWritable, ClusterMoviesWritable>> reducer,
						String distributedCacheFilePath, String inputPath, 
						String outputPath) throws IOException {
			
			super(conf, jobName);
			
			setMapperClass(mapper);
			setReducerClass(reducer);

			setOutputKeyClass(CentroidWritable.class);
			setOutputValueClass(ClusterMoviesWritable.class);
			setMapOutputKeyClass(IntWritable.class);
			setMapOutputValueClass(FeatureWritable.class);

			setInputFormatClass(TextInputFormat.class);
			setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.addInputPath(this, new Path(inputPath));
			FileOutputFormat.setOutputPath(this, new Path(outputPath));
			
		}
		
	}
}


