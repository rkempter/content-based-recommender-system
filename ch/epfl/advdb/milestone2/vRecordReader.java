package ch.epfl.advdb.milestone2;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class vRecordReader extends RecordReader<LongWritable, Text> {
    private static final int BUFFER_SIZE = 11;
    private static final String DELIMITER = Constants.TEXT_SEPARATOR;

    private StringBuffer valueBuffer = new StringBuffer();
    private Long[] keyBuffer = new Long[BUFFER_SIZE];
    private LongWritable key = new LongWritable();
    private Text value = new Text();

    private RecordReader<LongWritable, Text> rr;
    
    public vRecordReader(RecordReader<LongWritable, Text> rr) {
        this.rr = rr;
    }

    @Override
    public void close() throws IOException {
        rr.close();
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return rr.getProgress();
    }

    @Override
    public void initialize(InputSplit arg0, TaskAttemptContext arg1)
            throws IOException, InterruptedException {
        rr.initialize(arg0, arg1);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
    	valueBuffer = new StringBuffer();
        for(int i = 0; i < BUFFER_SIZE; i++) {
        	if (rr.nextKeyValue()) {
            	String[] lineElements = rr.getCurrentValue().toString().trim().split(Constants.TEXT_SEPARATOR);
            	int row = Integer.parseInt(lineElements[1]) - 1;
            	int column = Integer.parseInt(lineElements[2]);
            	valueBuffer.append(column);
            	if(lineElements[0].equals("V")) {
            		keyBuffer[row] = rr.getCurrentKey().get();
            		valueBuffer.append(Constants.TEXT_SEPARATOR);
                    valueBuffer.append(lineElements[3]);
            	}
            } else {
                return false;
            }
        }
        
        key.set(keyBuffer[0]);
        value.set(valueBuffer.toString());
        return true;
    }

}
