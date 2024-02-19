import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ToolReducer extends Reducer<Text, Text, Text, Text> {
    private static final long MAX_OUTPUT_SIZE = 128 * 1024 * 1024; // 128MB
    private long currentOutputSize = 0;
    private int fileCounter = 0;
    private long blocksize = 0;
    private Path outputDir;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        blocksize = conf.getLong("dfs.blocksize", 128 * 1024 * 1024); // Default block size is 128MB
        outputDir = FileOutputFormat.getOutputPath(context);
    }

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // Concatenate all values for the same key in the correct order
        StringBuilder mergedRecord = new StringBuilder();
        for (Text value : values) {
            mergedRecord.insert(0, value.toString() + "\n");  // Insert at the beginning to maintain order
        }

        // Check the size of the merged record
        long mergedRecordSize = mergedRecord.toString().getBytes().length;

        // Ignore files that are too big (more than 10% of block size)
        if (mergedRecordSize > 0.10 * blocksize) {
            return;
        }

        // If adding the current record exceeds the size limit, create a new output file
        if (currentOutputSize + mergedRecordSize > MAX_OUTPUT_SIZE || currentOutputSize + mergedRecordSize > blocksize) {
            fileCounter++;
            context.write(new Text(outputDir + "/MergedFiles/OutputFile_" + fileCounter), new Text(mergedRecord.toString()));
            currentOutputSize = mergedRecordSize;
        } else {
            context.write(new Text(outputDir + "/MergedFiles/OutputFile_" + fileCounter), new Text(mergedRecord.toString()));
            currentOutputSize += mergedRecordSize;
        }
    }
}
