package ch.epfl.advdb.milestone2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
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
	
	String inputDir, outputDir, testDir;
	boolean radiusTrue = false;
	int numOfIterations, numOfReducers, numOfMappers, numOfIMDBClusters, numOfNetflixClusters;
	
	public static enum CLUSTER_COUNTERS {
		NETFLIX_COUNTER,
		IMDB_COUNTER
	};
	
	public DataClustering(String inputDir, String outputDir, String testDir,
						  int numOfIterations, int numOfReducers,
						  int numOfMappers, int numOfIMDBClusters,
						  int numOfNetflixClusters) {
		
		this.inputDir = inputDir;
		this.testDir = testDir;
		this.outputDir = outputDir;
		this.numOfIterations = numOfIterations;
		this.numOfReducers = numOfReducers;
		this.numOfMappers = numOfMappers;
		this.numOfIMDBClusters = numOfIMDBClusters;
		this.numOfNetflixClusters = numOfNetflixClusters;
		
		configuration = new Configuration();
		configuration.set("mapred.textoutputformat.separator",
						  String.valueOf(Constants.TEXT_SEPARATOR));
		
		configuration.setInt("mapred.map.tasks", numOfMappers);
	}
	
	public void setRadius(boolean flag) {
		this.radiusTrue = flag;
	}
	
	public void createInitialCentroids() throws IOException {
		System.out.println("Apply k-means++ to imdb - Build initial centroids");
		IMDBCentroidBuilder icb = new IMDBCentroidBuilder(configuration, inputDir+"/features/", outputDir+"/../tmp/"+numOfIMDBClusters+"_imdb_centroids_0/centroids.dat", numOfIMDBClusters);
		icb.createInitialAdvancedCentroids();
		System.out.println("Apply k-means++ to netflix - Build initial centroids");
		NetflixCentroidBuilder ncb = new NetflixCentroidBuilder(configuration, inputDir+"/V/", outputDir+"/../tmp/"+numOfNetflixClusters+"_netflix_centroids_0/centroids.dat", numOfNetflixClusters);
		ncb.createInitialAdvancedCentroids();
	}
	
	@SuppressWarnings("deprecation")
	public void execute() 
			throws IOException, InterruptedException, ClassNotFoundException {
		
		int iter = 0;
		long lastNetflixCounter = 0;
		long lastImdbCounter = 0;
		boolean imdbConvergence = false;
		boolean netflixConvergence = false;
		int netflixFolderNumber = 1;
		int imdbFolderNumber = 1;

		createInitialCentroids();
		
		for(; iter < 10; iter++) {
			System.out.println("Run iteration: "+iter);
			
			if(!imdbConvergence) {
				{
					Configuration conf = new Configuration(configuration);
					conf.setInt("mapred.reduce.tasks", Constants.NUM_OF_REDUCERS);
					conf.setInt(Constants.FEATURE_DIMENSION_STRING, Constants.NUM_OF_FEATURES);
					conf.setInt(Constants.NBR_CLUSTERS, this.numOfIMDBClusters);
					conf.setInt(Constants.MATRIX_TYPE, Constants.IMDB_CLUSTER);
					
					FileSystem fs = FileSystem.get(conf);
					
					FileStatus[] fileStatus = fs.listStatus(new Path(outputDir+"/../tmp/"+numOfIMDBClusters+"_imdb_centroids_"+iter+"/"));
					for (FileStatus status : fileStatus) {
					    DistributedCache.addCacheFile(status.getPath().toUri(), conf);
					}
					
					DistributedCache.createSymlink(conf);
					
					DataClusteringJob imdbClustering = new DataClusteringJob(
							conf, "IMDB Clustering "+iter, "I",
							ClusterMapper.class, ClusterReducer.class,
							inputDir+"/features/",
							outputDir+"/../tmp/"+numOfIMDBClusters+"_imdb_centroids_"+(iter+1)+"/");
					
					
					imdbClustering.waitForCompletion(true);
					long imdbCounter = imdbClustering.getCounters().findCounter(CLUSTER_COUNTERS.IMDB_COUNTER).getValue() - lastImdbCounter;
					lastImdbCounter = imdbClustering.getCounters().findCounter(CLUSTER_COUNTERS.IMDB_COUNTER).getValue();
					if(imdbCounter == 0) {
						imdbConvergence = true;
					}
					imdbFolderNumber = iter + 1;
				}
			}
			
			if(!netflixConvergence) {
				{
					Configuration conf = new Configuration(configuration);
					conf.setInt("mapred.reduce.tasks", Constants.NUM_OF_REDUCERS);
					conf.setInt(Constants.FEATURE_DIMENSION_STRING, 10);
					conf.setInt(Constants.NBR_CLUSTERS, this.numOfNetflixClusters);
					conf.setInt(Constants.MATRIX_TYPE, Constants.NETFLIX_CLUSTER);
					
					FileSystem fs = FileSystem.get(conf);
					
					FileStatus[] fileStatus = fs.listStatus(new Path(outputDir+"/../tmp/"+numOfNetflixClusters+"_netflix_centroids_"+iter+"/"));
					for (FileStatus status : fileStatus) {
					    DistributedCache.addCacheFile(status.getPath().toUri(), conf);
					}
					
					DistributedCache.createSymlink(conf);
					
					DataClusteringJob netflixClustering = new DataClusteringJob(
							conf, "Netflix Clustering "+iter, "N",
							ClusterMapper.class, ClusterReducer.class,
							inputDir+"/V/",
							outputDir+"/../tmp/"+numOfNetflixClusters+"_netflix_centroids_"+(iter+1)+"/");
					
					
					netflixClustering.waitForCompletion(true);
					long netflixCounter = netflixClustering.getCounters().findCounter(CLUSTER_COUNTERS.NETFLIX_COUNTER).getValue() - lastNetflixCounter;
					lastNetflixCounter = netflixClustering.getCounters().findCounter(CLUSTER_COUNTERS.NETFLIX_COUNTER).getValue();
					if(netflixCounter == 0) {
						netflixConvergence = true;
					}
					netflixFolderNumber = iter + 1;
				}
			}
		}
//		
		if(radiusTrue) {
			AvgClusterRadius acr_netflix = new AvgClusterRadius(Constants.NETFLIX_CLUSTER, numOfNetflixClusters, configuration, inputDir+"/V/", outputDir+"/../tmp/"+numOfNetflixClusters+"_netflix_centroids_"+netflixFolderNumber+"/", outputDir+"/../tmp/radius/netflix.txt");
			float avgRadiusNetflix = acr_netflix.execute();
			acr_netflix.writeRadiusToFile(avgRadiusNetflix);
			AvgClusterRadius acr_imdb = new AvgClusterRadius(Constants.IMDB_CLUSTER, numOfIMDBClusters, configuration, inputDir+"/features/", outputDir+"/../tmp/"+numOfIMDBClusters+"_imdb_centroids_"+imdbFolderNumber+"/", outputDir+"/../tmp/radius/imdb.txt");
			float avgRadiusIMDB = acr_imdb.execute();
			acr_imdb.writeRadiusToFile(avgRadiusIMDB);
		}
		
		
		//IMDB: Assign the movie Id's to cluster
		{
			Configuration conf = new Configuration(configuration);
			conf.setInt("mapred.reduce.tasks", Constants.NUM_OF_REDUCERS);
			conf.setInt(Constants.NBR_CLUSTERS, this.numOfIMDBClusters);
			conf.setInt(Constants.MATRIX_TYPE, Constants.IMDB_CLUSTER);
			
			FileSystem fs = FileSystem.get(conf);
			
			FileStatus[] fileStatus = fs.listStatus(new Path(outputDir+"/../tmp/"+numOfIMDBClusters+"_imdb_centroids_"+imdbFolderNumber+"/"));
			for (FileStatus status : fileStatus) {
			    DistributedCache.addCacheFile(status.getPath().toUri(), conf);
			}
			
			DistributedCache.createSymlink(conf);
			
			MovieClusteringJob imdbMovieClustering = new MovieClusteringJob(
					conf, "IMDB Movie Clustering", "I",
					MovieMapper.class, MovieReducer.class,
					inputDir+"/features/",
					outputDir+"/../tmp/imdb_cluster_movies/");
			
			imdbMovieClustering.waitForCompletion(true);
		}
		
		// Netflix: Assign the movie Id's to cluster
		{
			Configuration conf = new Configuration(configuration);
			conf.setInt("mapred.reduce.tasks", Constants.NUM_OF_REDUCERS);
			conf.setInt(Constants.NBR_CLUSTERS, numOfNetflixClusters);
			conf.setInt(Constants.MATRIX_TYPE, Constants.NETFLIX_CLUSTER);
			
			FileSystem fs = FileSystem.get(conf);
			
			FileStatus[] fileStatus = fs.listStatus(new Path(outputDir+"/../tmp/"+numOfNetflixClusters+"_netflix_centroids_"+netflixFolderNumber+"/"));
			for (FileStatus status : fileStatus) {
			    DistributedCache.addCacheFile(status.getPath().toUri(), conf);
			}
			
			DistributedCache.createSymlink(conf);
			
			MovieClusteringJob netflixMovieClustering = new MovieClusteringJob(
					conf, "Netflix Movie Clustering", "N",
					MovieMapper.class, MovieReducer.class,
					inputDir+"/V/",
					outputDir+"/../tmp/netflix_cluster_movies/");
			
			netflixMovieClustering.waitForCompletion(true);
		}
		
		
		// Create Mapping between IMDB & Netflix
		{
			Configuration conf = new Configuration(configuration);
			conf.setInt("mapred.reduce.tasks", Constants.NUM_OF_REDUCERS);
			conf.setInt(Constants.NBR_CLUSTERS, numOfIMDBClusters);
			
			FileSystem fs = FileSystem.get(conf);
			
			FileStatus[] fileStatus = fs.listStatus(new Path(outputDir+"/../tmp/netflix_cluster_movies/"));
			for (FileStatus status : fileStatus) {
			    DistributedCache.addCacheFile(status.getPath().toUri(), conf);
			}
			
			DistributedCache.createSymlink(conf);
			
			MappingJob movieMapping = new MappingJob(conf, 
					"Create Mapping between Netflix & IMDB",
					MappingMapper.class,
					outputDir+"/../tmp/imdb_cluster_movies/",
					outputDir+"/../tmp/mapping/");
			
			movieMapping.waitForCompletion(true);
		}
		
		// Get new movies, cluster them
		{
			Configuration conf = new Configuration(configuration);
			conf.setInt("mapred.reduce.tasks", Constants.NUM_OF_REDUCERS);
			conf.setInt(Constants.MATRIX_TYPE, Constants.IMDB_CLUSTER);
			conf.setInt(Constants.NBR_CLUSTERS, numOfIMDBClusters);
			
			FileSystem fs = FileSystem.get(conf);
			
			FileStatus[] fileStatus = fs.listStatus(new Path(outputDir+"/../tmp/"+numOfIMDBClusters+"_imdb_centroids_"+imdbFolderNumber+"/"));
			for (FileStatus status : fileStatus) {
			    DistributedCache.addCacheFile(status.getPath().toUri(), conf);
			}
			fileStatus = fs.listStatus(new Path(outputDir+"/../tmp/mapping/"));
			for (FileStatus status : fileStatus) {
			    DistributedCache.addCacheFile(status.getPath().toUri(), conf);
			}
			
			DistributedCache.createSymlink(conf);
			
			MovieMappingClusteringJob movieMappingClustering = new MovieMappingClusteringJob(conf, 
					"Assign clusters to new movies",
					MovieMapper.class,
					newMovieReducer.class,
					testDir+"/features/", 
					outputDir+"/../tmp/new_movie_clusters/");
			
			movieMappingClustering.waitForCompletion(true);
		}
		
		 //Compute, which user could like each new movie
		{
			Configuration conf = new Configuration(configuration);
			conf.setInt("mapred.reduce.tasks", Constants.NUM_OF_REDUCERS);
			conf.setInt("mapred.map.tasks", Constants.NUM_OF_MAPPERS);
			conf.setInt(Constants.NBR_CLUSTERS, numOfNetflixClusters);
			
			
			FileSystem fs = FileSystem.get(conf);
			
			FileStatus[] fileStatus = fs.listStatus(new Path(outputDir+"/../tmp/"+numOfNetflixClusters+"_netflix_centroids_"+netflixFolderNumber+"/"));
			for (FileStatus status : fileStatus) {
			    DistributedCache.addCacheFile(status.getPath().toUri(), conf);
			}
			fileStatus = fs.listStatus(new Path(outputDir+"/../tmp/new_movie_clusters/"));
			for (FileStatus status : fileStatus) {
			    DistributedCache.addCacheFile(status.getPath().toUri(), conf);
			}
			
			DistributedCache.createSymlink(conf);
			
			UserJob userMapping = new UserJob(conf, 
					"Output users that could like the movies", 
					UserMapper.class,
					UserReducer.class,
				    inputDir+"/U/", 
				    outputDir);
			
			userMapping.waitForCompletion(true);
		}
		
		// Compute F-Score
		{
			Configuration conf = new Configuration(configuration);
			
			FileSystem fs = FileSystem.get(conf);
			
			FileStatus[] fileStatus = fs.listStatus(new Path(outputDir));
			for (FileStatus status : fileStatus) {
			    DistributedCache.addCacheFile(status.getPath().toUri(), conf);
			}
			
			DistributedCache.createSymlink(conf);
			
			ScoreJob userMapping = new ScoreJob(conf, 
					"Computing f-score", 
					ScoreMapper.class,
					ScoreReducer.class,
				    testDir+"/ratings/", 
				    outputDir+"/../tmp/fscore");
			
			userMapping.waitForCompletion(true);
		}
	
	}
	
	public void setNumberOfIMDBClusters(int nbr) {
		numOfIMDBClusters = nbr;
	}
	
	public void setNumberOfNetflixClusters(int nbr) {
		numOfNetflixClusters = nbr;
	}
	
	 /**DataClusteringJob
	 - MovieClusteringJob
	 - MappingJob
	 - MovieMappingClusteringJob
	 - UserJob
	 */
	
	private class DataClusteringJob extends Job {
		
		@SuppressWarnings("deprecation")
		public DataClusteringJob(Configuration conf, 
				String jobName, 
				String matrixType,
				Class<? extends Mapper<LongWritable, Text, 
						IntWritable, FeatureWritable>> mapper,
				Class<? extends Reducer<IntWritable, FeatureWritable, 
						Text, FeatureWritable>> reducer,
				String inputPath, 
				String outputPath) throws IOException {
			
			super(conf, jobName);
			
			setJarByClass(DataClusteringJob.class);
			
			if(matrixType.equals("N")) {
				setInputFormatClass(MatrixInputFormat.class);
			} else {
				setInputFormatClass(TextInputFormat.class);
			}

			setMapperClass(mapper);
			setReducerClass(reducer);

			setOutputKeyClass(Text.class);
			setOutputValueClass(FeatureWritable.class);
			setMapOutputKeyClass(IntWritable.class);
			setMapOutputValueClass(FeatureWritable.class);
			
			setNumReduceTasks(Constants.NUM_OF_REDUCERS);

			setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.addInputPath(this, new Path(inputPath));
			FileOutputFormat.setOutputPath(this, new Path(outputPath));
			
		}
	}
	
	private class MovieClusteringJob extends Job {
		
		public MovieClusteringJob(Configuration conf, 
				String jobName, 
				String matrixType,
				Class<? extends Mapper<LongWritable, Text, 
						IntWritable, IntWritable>> mapper,
				Class<? extends Reducer<IntWritable, IntWritable, 
						IntWritable, Text>> reducer,
				String inputPath, 
				String outputPath) throws IOException {
			
			super(conf, jobName);
			
			setJarByClass(MovieClusteringJob.class);
			
			setMapperClass(mapper);
			setReducerClass(reducer);

			setOutputKeyClass(IntWritable.class);
			setOutputValueClass(Text.class);
			setMapOutputKeyClass(IntWritable.class);
			setMapOutputValueClass(IntWritable.class);
			
			setNumReduceTasks(Constants.NUM_OF_REDUCERS);

			if(matrixType.equals("N")) {
				setInputFormatClass(MatrixInputFormat.class);
			} else {
				setInputFormatClass(TextInputFormat.class);
			}
			setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.addInputPath(this, new Path(inputPath));
			FileOutputFormat.setOutputPath(this, new Path(outputPath));
			
		}
	}
	
	private class MappingJob extends Job {
		
		@SuppressWarnings("deprecation")
		public MappingJob(Configuration conf, 
				String jobName,
				Class<? extends Mapper<LongWritable, Text, 
						IntWritable, IntWritable>> mapper,
				String inputPath, 
				String outputPath) throws IOException {
			
			super(conf, jobName);
			
			setJarByClass(MappingJob.class);
			
			setMapperClass(mapper);
			setReducerClass(Reducer.class);

			setOutputKeyClass(IntWritable.class);
			setOutputValueClass(IntWritable.class);
			
			setNumReduceTasks(Constants.NUM_OF_REDUCERS);

			setInputFormatClass(TextInputFormat.class);
			setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.addInputPath(this, new Path(inputPath));
			FileOutputFormat.setOutputPath(this, new Path(outputPath));
			
		}
	}
	
	private class MovieMappingClusteringJob extends Job {
		
		@SuppressWarnings("deprecation")
		public MovieMappingClusteringJob(Configuration conf, 
				String jobName,
				Class<? extends Mapper<LongWritable, Text, 
						IntWritable, IntWritable>> mapper,
				Class<? extends Reducer<IntWritable, IntWritable, 
						IntWritable, Text>> reducer,
				String inputPath, 
				String outputPath) throws IOException {
			
			super(conf, jobName);
			
			setJarByClass(MovieMappingClusteringJob.class);

			setMapperClass(mapper);
			setReducerClass(reducer);

			setOutputKeyClass(IntWritable.class);
			setOutputValueClass(Text.class);
			setMapOutputKeyClass(IntWritable.class);
			setMapOutputValueClass(IntWritable.class);
			
			setNumReduceTasks(Constants.NUM_OF_REDUCERS);

			setInputFormatClass(TextInputFormat.class);
			setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.addInputPath(this, new Path(inputPath));
			FileOutputFormat.setOutputPath(this, new Path(outputPath));
			
		}
	}
	
	private class UserJob extends Job {
		
		@SuppressWarnings("deprecation")
		public UserJob(Configuration conf, 
				String jobName, 
				Class<? extends Mapper<LongWritable, Text, 
						IntWritable, IntWritable>> mapper,
				Class<? extends Reducer<IntWritable, IntWritable, 
						IntWritable, Text>> reducer,
			    String inputPath, 
			    String outputPath) throws IOException {
			
			super(conf, jobName);
			
			setJarByClass(UserJob.class);
			
			setInputFormatClass(MatrixInputFormat.class);

			setMapperClass(mapper);
			setReducerClass(reducer);

			setOutputKeyClass(IntWritable.class);
			setOutputValueClass(Text.class);
			setMapOutputKeyClass(IntWritable.class);
			setMapOutputValueClass(IntWritable.class);
			
			setNumReduceTasks(Constants.NUM_OF_REDUCERS);

			setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.addInputPath(this, new Path(inputPath));
			FileOutputFormat.setOutputPath(this, new Path(outputPath));
		}
	}
	
private class ScoreJob extends Job {
		
		@SuppressWarnings("deprecation")
		public ScoreJob(Configuration conf, 
				String jobName,
				Class<? extends Mapper<LongWritable, Text, 
						IntWritable, IntWritable>> mapper,
				Class<? extends Reducer<IntWritable, IntWritable, 
						IntWritable, FloatWritable>> reducer,
				String inputPath, 
				String outputPath) throws IOException {
			
			super(conf, jobName);
			
			setJarByClass(ScoreJob.class);

			setMapperClass(mapper);
			setReducerClass(reducer);

			setOutputKeyClass(IntWritable.class);
			setOutputValueClass(FloatWritable.class);
			setMapOutputKeyClass(IntWritable.class);
			setMapOutputValueClass(IntWritable.class);
			
			setNumReduceTasks(Constants.NUM_OF_REDUCERS);

			setInputFormatClass(TextInputFormat.class);
			setOutputFormatClass(TextOutputFormat.class);

			FileInputFormat.addInputPath(this, new Path(inputPath));
			FileOutputFormat.setOutputPath(this, new Path(outputPath));
			
		}
	}
}


