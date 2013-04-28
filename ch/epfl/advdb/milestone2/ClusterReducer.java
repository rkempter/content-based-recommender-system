package ch.epfl.advdb.milestone2;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

public class ClusterReducer extends 
	Reducer<IntWritable, ClusterFeatureWritable, ClusterFeatureWritable, ClusterMoviesWritable>{
	
	public void reduce(IntWritable key, 
			Iterable<ClusterFeatureWritable> values, Context context) {
		
	}
	
}
