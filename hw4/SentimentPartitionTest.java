package stubs;

import static org.junit.Assert.assertEquals;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

public class SentimentPartitionTest {

	SentimentPartitioner mpart;

	@Test
	public void testSentimentPartition() {

		mpart=new SentimentPartitioner();
		mpart.setConf(new Configuration());
		
		
		/*
		 * Test the words "love", "deadly", and "zodiac". 
		 * The expected outcomes should be 0, 1, and 2. 
		 */
        
 		/*
		 * TODO implement
		 */          
		int result1 = mpart.getPartition(new Text("love"), new IntWritable(1), 3);
		int result2 = mpart.getPartition(new Text("deadly"), new IntWritable(1), 3);
		int result3 = mpart.getPartition(new Text("zodiac"), new IntWritable(1), 3);
	
		assertEquals(result1, 0);
		assertEquals(result2, 1);
		assertEquals(result3, 2);
	}

}

