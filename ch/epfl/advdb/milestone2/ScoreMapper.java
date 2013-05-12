package ch.epfl.advdb.milestone2;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

public class ScoreMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>{

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		String[] values = value.toString().trim().split(Constants.TEXT_SEPARATOR);
		
		int user = Integer.parseInt(values[0]);
		int movie = Integer.parseInt(values[1]);
		
		if(Float.parseFloat(values[2]) >= 0) {
			context.write(new IntWritable(movie), new IntWritable(user));
		}
	}
	
}
