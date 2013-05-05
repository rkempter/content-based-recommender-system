package ch.epfl.advdb.milestone2;

public class Constants {

	public static final int NUM_OF_MAPPERS = 1;
	public static final int NUM_OF_REDUCERS = 1;
	
	public static final int NUM_OF_ITERATIONS = 10;
	
	public static final int NUM_OF_FEATURES = 1852;
	public static final int NUM_OF_NETFLIX_FEATURES = 10;
	
	public static final String NETFLIX_U_INPUT_DIR = "/netflix/input/U/";
	public static final String NETFLIX_V_INPUT_DIR = "/netflix/input/V/";
	
	public static final String NETFLIX_CENTROID_OUTPUT_DIR = "/output/netflix/centroids/";
	public static final String NETFLIX_CLUSTER_OUTPUT_DIR = "/output/netflix/cluster/";
	
	public static final String IMDB_FEATURE_INPUT_DIR = "/imdb/input/features/";
	
	public static final String IMDB_CENTROID_OUTPUT_DIR = "/imdb/output/centroids/";
	public static final String IMDB_CLUSTER_OUTPUT_DIR = "/imdb/output/cluster/";
	
	public static final String TEXT_SEPARATOR = ",";
	
	public static final String FEATURE_DIMENSION_STRING = "feature.dimensions";
	public static final String MATRIX_TYPE = "matrix.type";
	
	public static final int IMDB_CLUSTER = 1;
	public static final int NETFLIX_CLUSTER = 2;
}
