package ch.epfl.advdb.milestone2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Writable;

public class FeatureWritable implements Writable{

	private int id;
	private int featureNumber;
	private float featureValue;
	
	public FeatureWritable() {
		this.id = 0;
		this.featureValue = 0;
		this.featureNumber = 0;
	}
	
	public FeatureWritable(int movie, int featureNumber, float featureValue) {
		this.id = movie;
		this.featureNumber = featureNumber;
		this.featureValue = featureValue;
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeInt(id);
		out.writeInt(featureNumber);
		out.writeFloat(featureValue);
	}
	
	public void readFields(DataInput in) throws IOException {
		id = in.readInt();
		featureNumber = in.readInt();
		featureValue = in.readFloat();
	}
	
	public String toString() {
		return id + Constants.TEXT_SEPARATOR + featureNumber + Constants.TEXT_SEPARATOR + featureValue;
	}
	
	public void fromString(String row) {
		
		String regexpPattern = ".*([0-9]+),([0-9]+),(-[0-9]+.?[0-9]).*";
		Matcher matcher = Pattern.compile(regexpPattern).matcher(row);

		if(!matcher.matches()) {
			return;
		}
	
		id = Integer.parseInt(matcher.group(1).trim());
		featureNumber = Integer.parseInt(matcher.group(2).trim());
		featureValue = Float.parseFloat(matcher.group(3).trim());
	}

	public int getId() {
		return id;
	}

	public void setMovie(int movie) {
		this.id = movie;
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
