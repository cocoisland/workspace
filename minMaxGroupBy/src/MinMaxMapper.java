import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MinMaxMapper extends Mapper<LongWritable, Text, Text, Text> {
	// input: empid empname deptId salary
	
	String empSal = null;
	Text outputKey = new Text();
	Text outputVal = new Text();
	
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String[] fields = value.toString().split("\\W+");
    
    if (fields.length == 4 ){
    	empSal = fields[0].toString()+":"+fields[3].toString();
    	outputVal.set(empSal);
    	outputKey.set(fields[2]); //deptId key
    	context.write(outputKey, outputVal);

    }
    

  }
}
