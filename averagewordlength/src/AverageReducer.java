import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
	  
	  double num = 0;
	  double total = 0;
	  for(IntWritable value : values) {
		  num++;
		  total += value.get();
	  }
	  if (num > 0d ){
	  context.write(key, new DoubleWritable(total/num));
	  }
  }
}