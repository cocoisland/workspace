import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MinMaxReducer extends Reducer<Text, Text, Text, Text> {

	
  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
	  
	  double min = Double.MAX_VALUE;
	  double max = Double.MIN_VALUE;
	  double tempValue = 0D;
	  String empIdMax = "9999";
	  for(Text value : values) {
		  String[] fields = value.toString().split(":");
		  if (fields.length == 2){
			  tempValue = Double.parseDouble(fields[1]);
		  }
		  if ( tempValue > max ){
			  max = tempValue;
			  empIdMax = fields[0];
		  }
		  if ( tempValue < min){
			  min = tempValue;
		  }
		  
	  }
	  String minStr = Double.toString(min);
	  String maxStr = Double.toString(max);
	  
	  context.write(key, new Text(minStr+"\t"+maxStr));
	  context.write(new Text(empIdMax), new Text(key.toString()+"\t"+maxStr));
	 
	  
  }
}