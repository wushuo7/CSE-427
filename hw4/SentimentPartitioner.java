package stubs;

import java.util.HashSet;




import java.util.Set;
import java.io.File;  
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;  
import java.io.BufferedReader;  
import java.io.FileInputStream;  

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;
// some of code i used in this java doc are "how to use java to read a txt document"
// this reading method is based on the method I found online
// website: http://blog.csdn.net/nickwong_/article/details/51502969
public class SentimentPartitioner extends Partitioner<Text, IntWritable> implements
    Configurable {

  private Configuration configuration;
  Set<String> positive = new HashSet<String>();
  Set<String> negative = new HashSet<String>();

  /*
   * set up the negative and positive string of set 
   * 
   */
  /**
   * Add the positive and negative words to the respective sets using the files 
   * positive-words.txt and negative-words.txt.
   */
  @Override
  public void setConf(Configuration configuration) {
	  this.configuration = configuration;
    /*
     * TODO implement if necessary
     */
	  String pathname1 = "/home/training/workspace/wordcount/src/stubs/positive-words.txt";
	  String pathname2 = "/home/training/workspace/wordcount/src/stubs/negative-words.txt";
	  File positivewords = new File("positive-words.txt");// add the positive file
	  File negativewords = new File("negative-words.txt");// add the negative file
	  
	try{
	  
	  
      BufferedReader brp = new BufferedReader(new FileReader(positivewords));  
       String linep;
       
       
      while ((linep =brp.readLine()) != null) {  
          if(linep.charAt(0)!=';'){
        	  positive.add(linep);
          		}
	  		}
      		
      		brp.close();
	  	}
	catch (FileNotFoundException e1) {
		// TODO Auto-generated catch block
		e1.printStackTrace();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	try{
		  
		  InputStreamReader reader1 = new InputStreamReader(  
	              new FileInputStream(negativewords));  
	      BufferedReader brn = new BufferedReader(reader1);  
	      String linen ;  
	      
	      while ((linen= brn.readLine())!= null) {  
	          if(linen.charAt(0)!=';'){
	        	  negative.add(linen);
	          		}
		  		}
	      		brn.close();
	      		reader1.close();
		  	}
	catch (FileNotFoundException e1) {
		// TODO Auto-generated catch block
		e1.printStackTrace();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	
	
	
	
  	}

  /**
   * Implement the getConf method for the Configurable interface.
   */
  @Override
  public Configuration getConf() {
    return configuration;
  }

  /**
   * You need to implement the getPartition method for a partitioner class.
   * This method receives the words as keys (i.e., the output key from the mapper.)
   * It should return an integer representation of the sentiment category
   * (positive, negative, neutral).
   * 
   * For this partitioner to work, the job configuration must have been
   * set so that there are exactly 3 reducers.
   */
  public int getPartition(Text key, IntWritable value, int numReduceTasks) {
    /*
     * TODO implement
     * Change the return 0 statement below to return the number of the sentiment 
     * category; use 0 for positive words, 1 for negative words, and 2 for neutral words. 
     * Use the sets of positive and negative words to find out the sentiment.
     *
     * Hint: use positive.contains(key.toString()) and negative.contains(key.toString())
     * If a word appears in both lists assume it is positive. That is, once you found 
     * that a word is in the positive list you do not need to check if it is in the 
     * negative list. 
     */
	  if (positive.contains(key.toString())){
		  return 0;
	  }
	  else if (negative.contains(key.toString())){
		  return 1;
	  }
	  else{
		  return 2;
	  }
  }
}

