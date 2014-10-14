import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/* 
 * To define a reduce function for your MapReduce job, subclass 
 * the Reducer class and override the reduce method.
 * The class definition requires four parameters: 
 *   The data type of the input key (which is the output key type 
 *   from the mapper)
 *   The data type of the input value (which is the output value 
 *   type from the mapper)
 *   The data type of the output key
 *   The data type of the output value
 */   
public class SumReducer extends Reducer<Text, Text, Text, IntWritable> {

 
  @Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int wordCount = 0;
		/**
		for (Text value : values) {
		  	wordCount ++;
		}
		**/
		
		while (values.iterator().hasNext()) {
			values.iterator().next();
			wordCount ++;
		}
		
		context.write(key, new IntWritable(wordCount));
	}
}