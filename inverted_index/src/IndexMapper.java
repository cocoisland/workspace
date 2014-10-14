import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class IndexMapper extends Mapper<Text, Text, Text, Text> {

	Text outputKey = new Text();
	Text outputValue = new Text();
  @Override
  public void map(Text key, Text value, Context context) throws IOException,
      InterruptedException {


    FileSplit fileSplit = (FileSplit) context.getInputSplit();
    Path filepath = fileSplit.getPath();
    String filename = filepath.getName();
    outputValue.set(filename + "@" + key.toString());
    
    String lines = value.toString();
    for(String word: lines.split("\\W+")){
    	if (word.length() > 0 ){
    		outputKey.set(word);
    		
    		context.write(outputKey, outputValue);
    	}
    }
    
  }
}