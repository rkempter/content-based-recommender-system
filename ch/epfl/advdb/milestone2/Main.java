package ch.epfl.advdb.milestone2;

import java.io.IOException;

public class Main {

	/*
	 * Main function that controls the complete programm flow
	 */
	public static void main(String args[]) throws IOException
	{
		
		// Test of reservoir sampling of centroids
		DataClustering dc = new DataClustering(args[0], args[1], 2, 1, 1, 20, 20);
		dc.initialIMDBCentroids();
		
		// Cluster V as well as the imdb feature dataset (max of 10 times)
		
		// Do map reduce on new movies and output list with users for each movie
	}
	
}
