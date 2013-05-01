package ch.epfl.advdb.milestone2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Writable;

public class MovieWritable implements Writable{

	private int movie;
	private int featureNumber;
	private float featureValue;
	
	public MovieWritable() {
		this.movie = 0;
		this.featureValue = 0;
		this.featureNumber = 0;
	}
	
	public MovieWritable(int movie, int featureNumber, float featureValue) {
		this.movie = movie;
		this.featureNumber = featureNumber;
		this.featureValue = featureValue;
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeInt(movie);
		out.writeInt(featureNumber);
		out.writeFloat(featureValue);
	}
	
	public void readFields(DataInput in) throws IOException {
		movie = in.readInt();
		featureNumber = in.readInt();
		featureValue = in.readFloat();
	}
	
	public String toString() {
		return movie + Constants.TEXT_SEPARATOR + featureNumber + Constants.TEXT_SEPARATOR + featureValue;
	}
	
	public void fromString(String row) {
		
		String regexpPattern = ".*([0-9]+),([0-9]+),(-[0-9]+.?[0-9]).*";
		Matcher matcher = Pattern.compile(regexpPattern).matcher(row);

		if(!matcher.matches()) {
			return;
		}
	
		movie = Integer.parseInt(matcher.group(1).trim());
		featureNumber = Integer.parseInt(matcher.group(2).trim());
		featureValue = Float.parseFloat(matcher.group(3).trim());
	}

	public int getMovie() {
		return movie;
	}

	public void setMovie(int movie) {
		this.movie = movie;
	}

	public int getFeatureNumber() {
		return featureNumber;
	}

	public void setFeatureNumber(int featureNumber) {
		this.featureNumber = featureNumber;
	}

	public float getFeatureValue() {
		return featureValue;
	}

	public void setFeatureValue(float featureValue) {
		this.featureValue = featureValue;
	}
}
