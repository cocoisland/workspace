import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.sql.Date;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertThat ;
import static org.hamcrest.CoreMatchers.is;

public class TestFileObj {
  private Configuration conf;
  private String localPath="/home/training/testfile";
  private String hdfsPath="hdfs://localhost/user/training/testfile";
  
  @Before
  public void setUp() throws IOException {
	  conf = new Configuration();
	
  }
  
  @Test
  public void TestExistListStatus() throws IOException {
	  String renamedFile = hdfsPath + System.currentTimeMillis();
	  Path renamedPath = new Path(renamedFile);
	  Path f = new Path(hdfsPath);
	  FileSystem fs = FileSystem.get(URI.create(hdfsPath),conf);
	  if (fs.exists(f)){
		  FileStatus[] fstat = fs.listStatus(f);
		  if ( fstat.length == 1 && fstat[0].isFile() ) {
			  if (fstat[0].getModificationTime() > (System.currentTimeMillis() - 3600000)){
				  System.out.println("File is less than 1 hour old. Do nothing. Exiting");
				  System.exit(0);
				  
			  } else {
				  fs.rename(f, renamedPath);
				  FileSystem fsIn = FileSystem.get(URI.create(localPath),conf);
				  FSDataInputStream fin = fsIn.open(new Path(localPath));
				  FSDataOutputStream fout = fs.create(f);
				  IOUtils.copyBytes(fin, fout, 4096, true);
				  System.out.println("Create new file :" + hdfsPath);
				  System.out.println("Rename existing file to :" + renamedFile);
			  }
		  } else if ( fstat.length > 0 && fstat[0].isDirectory()){
			  System.out.println("Can not create file because directory exists at path :"
					  	 + hdfsPath);
		  }
		  
	  } 
  }
  /**
  @Test
  public void TestHdfsRead() throws IOException  {
	  //create filesystem instance

	  FSDataInputStream in = null;
	  try {
		  FileSystem fs = FileSystem.get(URI.create(hdfsPath), conf);
		  in = fs.open(new Path(hdfsPath)); 	//open inputstream
		  IOUtils.copyBytes(in, System.out,4096,false); 	//false means not closing stream & system.out
	  } finally {
		  IOUtils.closeStream(in);
		  in.close();
	  }
	  
  }
  
  @Test
  public void Testlocal2Hdfs() throws IOException {
	  org.apache.hadoop.fs.FileSystem fs = null;
	  org.apache.hadoop.fs.FSDataOutputStream  hout = null;
	  java.io.OutputStream out = null;
	  java.io.InputStream in = null;
	  
	  String localSrc = "/home/training/testfile";
	  String dst = "hdfs://localhost/user/training/tout99";
	 
	  in = new BufferedInputStream(new FileInputStream(localSrc));
	  
	  fs = FileSystem.get(URI.create(dst),conf);
		  //org.apache.hadoop.fs.FSDataOutputStream subclass of java.io.OutputStream
	  out = fs.create(new Path(dst), new Progressable() {
			  public void progress() {
				  System.out.println(".");
			  }
		  });
		  IOUtils.copyBytes(in, out, 4096, true);
		  IOUtils.closeStream(in);
		  out.close();
		  in.close();
		 
	  }
  
  @Test
  public void TestFileStatus() throws IOException {
	  FileSystem fs = FileSystem.get(URI.create(hdfsPath),conf);
	  org.apache.hadoop.fs.FileStatus stat = fs.getFileStatus(new Path(hdfsPath));
	  assertThat(stat.getPath().toUri().getPath(), is("/user/training/testfile"));
	  assertThat(stat.isDirectory(), is(false));
	  assertThat(stat.getOwner(), is("training"));
	  
  }
  **/
  
}

