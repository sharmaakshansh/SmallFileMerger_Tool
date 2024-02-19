import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class ToolMapper extends Mapper<LongWritable, Text, Text, Text> {
	private Text outKey = new Text();
    private long blockSize;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        blockSize = conf.getLong("dfs.blocksize", 128 * 1024 * 1024); // Default block size is 128MB
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fileName = fileSplit.getPath().getName();

        // Check if the file size exceeds the threshold
        if (ToolUtility.shouldSkipFile(fileSplit, blockSize)) {
            return;
        }

        outKey.set(fileName);
        context.write(outKey, value);
    }

 }

