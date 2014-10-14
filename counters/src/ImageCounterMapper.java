import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Example input line:
 * 96.7.4.14 - - [24/Apr/2011:04:20:11 -0400] "GET /cat.jpg HTTP/1.1" 200 12433
 *
 */
public class ImageCounterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String[] logRequest = value.toString().split("\"");
    if (logRequest.length > 1 ){
    	String[] reqSplit = logRequest[1].split("\\s+");
    	if (reqSplit.length > 1 ) {
    	String imageType = reqSplit[1].toLowerCase();
    	
    	if (imageType.length() > 1){
    		if (imageType.endsWith("jpg")){
    			context.getCounter("ImageCounter","jpeg").increment(1);
    		} else if (imageType.endsWith("gif")) {
    			context.getCounter("ImageCounter","gif").increment(1);
    		} else {
    			context.getCounter("ImageCounter","all other").increment(1);
    		}
    	}
    	}
    }
  }
}