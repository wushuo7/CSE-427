package stubs;


import org.apache.hadoop.util.Tool; 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;



/* 
 * MapReduce jobs are typically implemented by using a driver class.
 * The purpose of a driver class is to set up the configuration for the
 * MapReduce job and to run the job.
 * Typical requirements for a driver class include configuring the input
 * and output data formats, configuring the map and reduce classes,
 * and specifying intermediate data formats.
 * 
 * The following is the code for the driver class:
 */
public class AggreagteRatings extends Configured implements Tool{
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Configuration(), new AggreagteRatings(),args);
		System.exit(exitCode); 
		 }

  
    
	public int run(String[] args) throws Exception  { 
    if (args.length != 2) {
      System.out.printf(
          "Usage: AggregateByKeyDriver <input dir> <output dir>\n");
     return -1;
    }

    
    
    Job job = new Job(getConf());
    
    job.setJarByClass(AggreagteRatings.class);
    
    
    /*
     * Specify an easily-decipherable name for the job.
     * This job name will appear in reports and logs.
     */
    job.setJobName("AggregateByKeyDriver");

    /*
     * Specify the paths to the input and output data based on the
     * command-line arguments.
     */
    FileInputFormat.setInputPaths(job, new Path(args[0]));

    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    /*
     * Specify the mapper and reducer classes.
     */
    job.setMapperClass(AggreagteRatingsMapper.class);
    job.setReducerClass(SumReducer.class);
    job.setCombinerClass(SumReducer.class);
   
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    

    
    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;

  }


}


