import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCoMapper extends Mapper<Text, Text, Text, IntWritable> {

	Text outKey = new Text();
	IntWritable one = new IntWritable(1);
	
  @Override
  public void map(Text key, Text value, Context context)
      throws IOException, InterruptedException {
    
	String prev = null;
    String lines = value.toString().toLowerCase();
    for(String word : lines.split("\\W+")) {
    	if (word.length() > 0){
    		if (prev != null) {
    			outKey.set(prev + "," + word);
    			context.write(outKey, one);
    		}
    		prev = word;
    	}
    }
    
  }
}
