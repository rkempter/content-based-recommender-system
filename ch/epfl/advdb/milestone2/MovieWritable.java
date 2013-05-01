package ch.epfl.advdb.milestone2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class MovieWritable implements Writable{

	private int movie;
	private int feature;
	
	public MovieWritable() {
		this.movie = 0;
		this.feature = 0;
	}
	
	public MovieWritable(int movie, int feature) {
		this.movie = movie;
		this.feature = feature;
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeInt(movie);
		out.writeInt(feature);
	}
	
	public void readFields(DataInput in) throws IOException {
		movie = in.readInt();
		feature = in.readInt();
	}
	
	public String toString() {
		return movie + Constants.TEXT_SEPARATOR + feature;
	}
	
	public void fromString(String row) {
		int comma = row.indexOf(Constants.TEXT_SEPARATOR);
		if(comma == -1) return;
		movie = Integer.parseInt(row.substring(0, comma));
		feature = Integer.parseInt(row.substring(comma+1));
	}

	public int getMovie() {
		return movie;
	}

	public void setMovie(int movie) {
		this.movie = movie;
	}

	public int getFeature() {
		return feature;
	}

	public void setFeature(int feature) {
		this.feature = feature;
	}
}
