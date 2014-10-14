import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TwoJSONDriver extends Configured implements Tool {

  public int run(String[] args) throws Exception {

    if (args.length != 3) {
      System.out.printf("Usage: twoJSON <input> <input> <output>\n");
      return -1;
    }

    JobConf conf = new JobConf(getConf(),TwoJSONDriver.class );
    conf.setJobName(this.getClass().getName());
    
    MultipleInputs.addInputPath(conf, new Path(args[0]), TextInputFormat.class, AdEventMapper.class);
    MultipleInputs.addInputPath(conf, new Path(args[1]), TextInputFormat.class, AssetMapper.class);
    FileOutputFormat.setOutputPath(conf, new Path(args[2]));
    
    /**
     * Danger:
     * Never setMapperClass with MultipleInputs.
     * It doesn't cause error, but it confuses the input data with respect 
     * to input class. The following code will always run AssetMapper, then AdEventMapper last 
     * grabbing whatever last input data, even
     * though they have already been run above. 
     */
   //conf.setMapperClass(AssetMapper.class); 
    //conf.setMapperClass(AdEventMapper.class);
    conf.setReducerClass(pvJoinReducer.class);
    
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(Text.class);
    
    JobClient.runJob(conf);
    return 0;
    
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Configuration(), new TwoJSONDriver(), args);
    System.exit(exitCode);
  }
}
