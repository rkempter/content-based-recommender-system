package ch.epfl.advdb.milestone2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

public class Main {

	/*
	 * Main function that controls the complete programm flow
	 */
	public static void main(String args[]) throws IOException, InterruptedException, ClassNotFoundException {
		
		// Test of reservoir sampling of centroids
		DataClustering dc = new DataClustering(args[0], args[2], args[1], 2, Constants.NUM_OF_MAPPERS, Constants.NUM_OF_REDUCERS, 80, 80);
		
//		for(int i = 60; i < 110; i = i + 5) {
//			dc.setNumberOfIMDBClusters(i);
//			dc.setNumberOfNetflixClusters(i);
			dc.execute();
//			if(args[4] != null) {
//				dc.setRadius(true);
//			}
//		}		
	}
	
}
