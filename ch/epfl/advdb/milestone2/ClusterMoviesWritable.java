package ch.epfl.advdb.milestone2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class ClusterMoviesWritable implements Writable {
	int cluster;
	int movieId;
	
	public ClusterMoviesWritable(int cluster, int movieId) {
		this.cluster = cluster;
		this.movieId = movieId;
	}

	public void readFields(DataInput in) throws IOException {
		cluster = in.readInt();
		movieId = in.readInt();
	}

	public void write(DataOutput out) throws IOException {
		out.writeInt(cluster);
		out.writeInt(movieId);
	}

	public int getCluster() {
		return cluster;
	}

	public void setCluster(int cluster) {
		this.cluster = cluster;
	}

	public int getMovieId() {
		return movieId;
	}

	public void setMovieId(int movieId) {
		this.movieId = movieId;
	}
	
	public String toString() {
		return cluster + Constants.TEXT_SEPARATOR + movieId;
	}
	
	public void fromString(String row) {
		int comma = row.indexOf(Constants.TEXT_SEPARATOR);
		cluster = Integer.parseInt(row.substring(0, comma).trim());
		movieId = Integer.parseInt(row.substring(comma+1).trim());
	}
}