import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ImageCounter extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {

    if (args.length != 2) {
      System.out.printf("Usage: ImageCounter <input dir> <output dir>\n");
      return -1;
    }

    Job job = new Job(getConf());
    job.setJarByClass(ImageCounter.class);
    job.setJobName("Image Counter");

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setMapperClass(ImageCounterMapper.class);
    job.setNumReduceTasks(0);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    boolean success = job.waitForCompletion(true);
    if (success) {
    	System.out.println("ImageCounter.jpeg = "+
    			  job.getCounters().findCounter("ImageCounter","jpeg").getValue());
    	System.out.println("ImageCounter.gif = "+
    		      job.getCounters().findCounter("ImageCounter","gif").getValue());
    	System.out.println("ImageCounter.others = "+
    		      job.getCounters().findCounter("ImageCounter","all other").getValue());
      return 0;
     
    } else
      return 1;

  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Configuration(), new ImageCounter(), args);
    System.exit(exitCode);
  }
}