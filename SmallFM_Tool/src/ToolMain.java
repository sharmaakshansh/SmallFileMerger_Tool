//Final Minor Project (MCA 3rd SEM)
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;




public class ToolMain {
	public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setLong("dfs.blocksize", 128 * 1024 * 1024); // Set the default block size to 128MB
        conf.set("mapreduce.job.skiprecords", "true");
        
        // Set the boolean argument based on user input
        boolean deleteInputFiles = Boolean.parseBoolean(args[2]);
        conf.setBoolean("delete.input.files", deleteInputFiles);

        Job job = Job.getInstance(conf, "File Merge");

        job.setJarByClass(ToolMain.class);
        job.setMapperClass(ToolMapper.class);
        job.setReducerClass(ToolReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0])); // Input path

        // Create a subdirectory within the input directory for the output files
       // Path outputDir = new Path(args[0], "MergedFiles");
        //here we are setting a separate output path
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Create a ControlledJob to handle cleanup logic
        ControlledJob controlledJob = new ControlledJob(conf);
        controlledJob.setJob(job);

        // Create a JobControl and add the ControlledJob to it
        JobControl jobControl = new JobControl("FileMerge");
        jobControl.addJob(controlledJob);

        // Run the JobControl
        Thread jobControlThread = new Thread(jobControl);
        jobControlThread.start();

        // Wait for the job to complete
        while (!jobControl.allFinished()) {
            Thread.sleep(5000); // Sleep for 5 seconds before checking again
        }

        // Perform file operations (delete small files) if
        if (deleteInputFiles) {
            ToolUtility.deleteSmallFiles(conf, args[0],deleteInputFiles);
        }
    }
}
