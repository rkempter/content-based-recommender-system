package ch.epfl.advdb.milestone2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class vInputFormat extends TextInputFormat {
    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit arg0, TaskAttemptContext arg1) {
        return new vRecordReader(super.createRecordReader(arg0, arg1));
    }
}
