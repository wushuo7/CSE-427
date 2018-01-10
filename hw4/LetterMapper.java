package stubs;
import java.io.IOException;




import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LetterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	boolean caseSensitive=true;
	public void setup(Context context){
		  Configuration conf=context.getConfiguration();
		  caseSensitive = conf.getBoolean("caseSensitive",true);
	  }
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  
    /*
     * TODO implement
     */
	  String line = value.toString();
	  for (String word : line.split("\\W+")) {
		  if (word.length() > 0) {
			  if(caseSensitive){
				  String a = word.substring(0,1);
				  int b = word.length();
			  context.write(new Text(a), new IntWritable(b));
			  }
			  else{
			  String V = word.substring(0,1);
			  String a = V.toLowerCase();
			  int b = word.length();
		  context.write(new Text(a), new IntWritable(b));
			  }
		  }
	  }
  }
  
}
