import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JoinDriver extends Configured implements Tool {

  public int run(String[] args) throws Exception {

    if (args.length != 4) {
      System.out.printf("Usage: JoinDriver <input input> <output>\n");
      return -1;
    }

    Job job = new Job(getConf());
    job.setJarByClass(JoinDriver.class);
    job.setJobName("Three Join ");
    
    job.setMapperClass(WebLoginMapper.class);
    job.setMapperClass(FileVisitedMapper.class);
    job.setMapperClass(FileVisited1Mapper.class);
    job.setReducerClass(JoinReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    MultipleInputs.addInputPath(job, new Path(args[0]), KeyValueTextInputFormat.class, WebLoginMapper.class);
    MultipleInputs.addInputPath(job, new Path(args[1]), KeyValueTextInputFormat.class, FileVisitedMapper.class);
    MultipleInputs.addInputPath(job, new Path(args[2]), KeyValueTextInputFormat.class, FileVisited1Mapper.class);
    Path outputPath = new Path(args[3]);
    FileOutputFormat.setOutputPath(job, outputPath);
    outputPath.getFileSystem(getConf()).delete(outputPath,true);

    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new Configuration(), new JoinDriver(), args);
    System.exit(exitCode);
  }
}
