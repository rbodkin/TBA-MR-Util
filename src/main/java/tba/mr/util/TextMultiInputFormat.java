package tba.mr.util;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

public class TextMultiInputFormat extends org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat<LongWritable, Text> implements JobConfigurable {

    private CompressionCodecFactory compressionCodecs = null;
    private int numSplits;

    public void configure(JobConf conf) {
        compressionCodecs = new CompressionCodecFactory(conf);
        //numSplits = conf.get
    }

    protected boolean isSplitable(FileSystem fs, Path file) {
        return compressionCodecs.getCodec(file) == null;
    }

//    @Override
//    public org.apache.hadoop.mapred.InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
//        List<InputSplit> splits = getSplits((org.apache.hadoop.mapreduce.JobContext)new Job(job));
//        return splits.toArray(new org.apache.hadoop.mapred.InputSplit[splits.size()]);        
//    }

    public org.apache.hadoop.mapreduce.RecordReader<LongWritable, Text> createRecordReader(InputSplit split,
            TaskAttemptContext context) throws IOException {

        // looks like this constructor can't work without casting into a raw type - ouch
        return new CombineFileRecordReader((CombineFileSplit)split, context, CombineFileRecordReader.class);
    }

}
