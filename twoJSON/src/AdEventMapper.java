import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class AdEventMapper extends MapReduceBase 
	implements Mapper<LongWritable,Text,Text,Text>{

	Text keyOutput = new Text();
	Text valueOutput = new Text();
  @Override
  public void map(LongWritable key, Text value, 
		  OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
  
	  	int startIdx = value.toString().indexOf("\"pv\":")+6;
	  	int endIdx = value.toString().indexOf("\"", startIdx);
	  	String pvKey = value.toString().substring(startIdx, endIdx);
	  	
	  	startIdx = value.toString().indexOf("\"e\":")+5;
	  	endIdx = value.toString().indexOf("\"", startIdx);
	  	if (startIdx < endIdx){
	  	String eventType = value.toString().substring(startIdx, endIdx);
	  	
	  	keyOutput.set(pvKey);
	  	valueOutput.set(eventType);

	  	output.collect(keyOutput, valueOutput);
	  	}
  }
}