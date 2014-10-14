import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Example input line:
 * 96.7.4.14 - - [24/Apr/2011:04:20:11 -0400] "GET /cat.jpg HTTP/1.1" 200 12433
 *
 */
public class LogFileMapper extends Mapper<LongWritable, Text, Text, Text> {


  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  
    String[] field = value.toString().split("\\s");
    if (field.length > 3 ){
    	String month = field[3].split("\\/")[1];
    	if (month.length() > 1 ){
    		context.write(new Text(field[0]), new Text(month));
    	}
    }
    
    
  }
}