import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WebLoginMapper extends Mapper<Text, Text, Text, IntWritable> {
	//ip	date	text
	
	Text outputKey = new Text();
	Text outputValue = new Text();
  @Override
  public void map(Text key, Text value, Context context) throws IOException,
      InterruptedException {
	  
	  //Monday, December 25, 2014 20:15:05 PST
	SimpleDateFormat formatter = new SimpleDateFormat("EEEEE, MMMMM dd, yyyy HH:mm:ss z");
    
    String[] fields = value.toString().split("\t");
    if (fields.length == 3){
    	try {
			Date dt = formatter.parse(fields[0]);
			formatter.applyPattern("MM/dd/yyyy");
			String dtStr = formatter.format(dt);

			outputKey.set(dtStr+"\t"+key);
			context.write(outputKey, new IntWritable(1));
		
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
  
  }
}