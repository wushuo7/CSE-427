package stubs;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Example input line:
 * 96.7.4.14 - - [24/Apr/2011:04:20:11 -0400] "GET /cat.jpg HTTP/1.1" 200 12433
 *
 */
public class LogFileMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    /*
     * TODO implement
     */
	  String line = value.toString(); // get the input information
	  String b = line.substring(0, line.indexOf(" "));// get the index information
	  if (b.length() > 0) {
	       
	        /*
	         * Call the write method on the Context object to emit a key
	         * and a value from the map method.
	         */
	        context.write(new Text(b), new IntWritable(1));
  }	
	  }
	  
}