package stubs;
import java.io.IOException;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

public class IndexMapper extends Mapper<Text, Text, Text, Text> {

  @Override
  public void map(Text key, Text value, Context context) throws IOException,
      InterruptedException {

    /*
     * TODO implement
     */	
	  	
	  	FileSplit file_name = (FileSplit)context.getInputSplit();
	  	String line = value.toString();
	  	String filename = file_name.getPath().getName();
	  	Text fileName = new Text(filename);
     /*
     *  get the file name and connet the word and the filename & line that contain it
     */	
	  	String line1 = line.toLowerCase();
	    String [] sa=line1.split("\\W+");
	    for(int i=0;i<sa.length-1;i++){
	    	if(sa[i].length()>0){
	    			String word = sa[i];
	    			context.write(new Text(word), new Text(fileName+"@"+key));
	    	}
	    	}
  }
}