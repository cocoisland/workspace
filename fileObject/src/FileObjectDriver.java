import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class FileObjectDriver {
	String localPath="/home/training/testfile";
	public static final String hdfsPath="hdfs://localhost/user/training/minMaxInput";
	public static final String appendDst="hdfs://localhost/user/training/appendDst";
	
	public void fileCat(Configuration conf, Path obj) {
		FSDataInputStream in = null;
		FileSystem fs = null;
		try {		 
			//FSDataInputStream in = null;
			 fs = FileSystem.get(URI.create(obj.toString()),conf);
			if (fs.exists(obj) && fs.isFile(obj)){
				in = fs.open(obj);
				IOUtils.copyBytes(in, System.out, 4096, false);
			}		
			
		} catch(IOException iox) {
			System.out.println("FileCat Exception:"+iox.getMessage());
		} finally {
			try {
				in.close();
				fs.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
	}
	
	public void fileAppend(Configuration conf, Path mergeFrom, Path mergeTo){
		try {
			FileSystem fsIn = FileSystem.get(URI.create(mergeFrom.toString()),conf);
			FSDataInputStream in = fsIn.open(mergeFrom);
			FileSystem fsOut = FileSystem.get(URI.create(mergeTo.toString()),conf);
			if (! fsOut.exists(mergeTo)){
				fsOut.createNewFile(mergeTo);
			}
			FSDataOutputStream out = fsOut.append(mergeTo);
			IOUtils.copyBytes(in, out, 4096,false); 
			out.close();
			in.close();
			fsIn.close(); 
			fsOut.close();
			
		} catch (IOException e) {
			//System.out.println("fileAppend IOException:"+e.getMessage());
			e.printStackTrace();
		} finally {
			
		}
	}
	
	public void dirFilesMerge(Configuration conf, Path srcDir, Path dstFile) {
		try {
			FileSystem srcFS = FileSystem.get(URI.create(srcDir.toString()),conf);
			if (srcFS.exists(srcDir) && srcFS.isDirectory(srcDir)) {
				FileSystem dstFS = FileSystem.get(URI.create(dstFile.toString()),conf);
				if (dstFS.exists(dstFile)) {
					dstFS.delete(dstFile,true);
				}
				FileUtil.copyMerge(srcFS, srcDir, dstFS, dstFile, false, conf, "\nDone Merging\n");
			} else {
				System.out.println("Directory does not exist."+srcDir.toString());
			}
		} catch (IOException e) {
			System.out.println("FileMerge exception:"+e.getMessage());
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws Exception {
		Configuration conf= new Configuration();
		Path filePath = new Path(hdfsPath);
		Path appendPath = new Path(appendDst);
		FileObjectDriver fobj = new FileObjectDriver();
	
		
		System.out.println("System out HDFS inputfile"+hdfsPath);
		fobj.fileCat(conf, filePath);
		
		System.out.printf("Appending from %s to %s\n",hdfsPath, appendDst);
		fobj.fileAppend(conf, filePath, appendPath);
		fobj.fileCat(conf, appendPath);
	  /*
		String mergeDir = "hdfs://localhost/user/training/mergeDir";
		String dstFile = "hdfs://localhost/user/training/mergeDir/dstFile";
		Path mPath = new Path(mergeDir);
		Path dPath = new Path(dstFile);
		System.out.printf("MergeDir %s into dstPath %s\n",mPath,dPath);
		fobj.dirFilesMerge(conf, mPath, dPath);
		fobj.fileCat(conf, dPath);
		*/
	}
  }
