import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class ToolUtility {

	
	public static void deleteSmallFiles(Configuration conf, String inputPath, boolean deleteInputFiles) throws IOException {
		    FileSystem fs = FileSystem.get(conf);
		    FileStatus[] inputFiles = fs.listStatus(new Path(inputPath));

		    for (FileStatus inputFile : inputFiles) {
		        if (inputFile.isFile() && inputFile.getLen() < 0.10 * conf.getLong("dfs.blocksize", 128 * 1024 * 1024)) {
		            if (deleteInputFiles) {
		                fs.delete(inputFile.getPath(), false);
		            }
		        }
		    }
	}
	
	
	public static boolean shouldSkipFile(FileSplit fileSplit, long blockSize) {
		 long fileSize = fileSplit.getLength();
	        // Skip files that are too big (more than 10% of block size)
	        return fileSize > 0.10 * blockSize;
	}
}
