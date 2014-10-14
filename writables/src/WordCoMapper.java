import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCoMapper extends Mapper<Text, Text, StringPairWritable, LongWritable> {


	LongWritable one = new LongWritable(1);
	
  @Override
  public void map(Text key, Text value, Context context)
      throws IOException, InterruptedException {
    
	String prev = null;
    String lines = value.toString().toLowerCase();
    for(String word : lines.split("\\W+")) {
    	if (word.length() > 0){
    		if (prev != null) {
    			
    			context.write(new StringPairWritable(prev,word), one);
    		}
    		prev = word;
    	}
    }
    
  }
}
