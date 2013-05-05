package ch.epfl.advdb.milestone2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

public class Main {

	/*
	 * Main function that controls the complete programm flow
	 */
	public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException {
		
		// Test of reservoir sampling of centroids
		DataClustering dc = new DataClustering(args[0], args[2], 2, Constants.NUM_OF_MAPPERS, Constants.NUM_OF_REDUCERS, 5, 5);
		dc.execute();
		
		// Cluster V as well as the imdb feature dataset (max of 10 times)
		
		
		
		// Do map reduce on new movies and output list with users for each movie
	}
	
}
