import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Reducer;

/**
 * On input, the reducer receives a word as the key and a set
 * of locations in the form "play name@line number" for the values. 
 * The reducer builds a readable string in the valueList variable that
 * contains an index of all the locations of the word. 
 */
public class JoinReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
	 
	int count=0;
    for(IntWritable value : values){
    	count++;
    }
  
    context.write(key,new IntWritable(count));
    
  }
}