package stubs;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCoMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    
    /*
     * TODO implement
     */
    String line = value.toString();
    String line1 = line.toLowerCase();
    String [] sa=line1.split("\\W+");
    for(int i=0;i<sa.length-1;i++){
    	if(sa[i].length()>0 && sa[i+1].length()>0){
    			String word = sa[i];
    			String wordnext = sa[i+1];
    			String now = word+","+wordnext;
    			context.write(new Text(now), new IntWritable(1));
    	}
    	}
  }
}
