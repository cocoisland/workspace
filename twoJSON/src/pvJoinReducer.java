import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class pvJoinReducer extends MapReduceBase 
implements Reducer<Text,Text,Text,Text> {

	Text valueOutput = new Text();
	String assetKey = "Off";
	String valueStr = "";
	int viewNum = 0;
	int clickNum = 0;
	int assetNum = 0;

	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
					throws IOException {

		while (values.hasNext()){
			valueStr = values.next().toString();	 
			if (valueStr.equals("asset")){
				assetKey = key.toString();
				assetNum++;
			}
			if (key.toString().equals(assetKey) && 
					(valueStr.equals("view")) ) viewNum++;
			if (key.toString().equals(assetKey) && 
					(valueStr.equals("click")) ) clickNum++;
		}
		if (assetNum > 0){	
			valueOutput.set("\t"+assetNum+"\t"+viewNum+"\t"+clickNum);
			output.collect(key,valueOutput);
			assetNum=0;
			viewNum=0;
			clickNum=0;
		}
	}
}