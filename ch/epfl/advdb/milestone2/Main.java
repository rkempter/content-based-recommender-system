package ch.epfl.advdb.milestone2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

public class Main {

	/*
	 * Main function that controls the complete programm flow
	 */
	public static void main(String args[]) throws IOException
	{
		
		// Test of reservoir sampling of centroids
		DataClustering dc = new DataClustering(args[0], args[2], 2, 1, 1, 5, 5);
		dc.initialIMDBCentroids();
		
		Configuration conf = new Configuration();
		
		NetflixCentroidBuilder ncb = new NetflixCentroidBuilder(conf, args[0] + "/V", args[2] + "/netflix_centroids/centroids.dat", 10);
		ncb.createInitialCentroids();
		
		// Cluster V as well as the imdb feature dataset (max of 10 times)
		
		
		
		// Do map reduce on new movies and output list with users for each movie
	}
	
}
