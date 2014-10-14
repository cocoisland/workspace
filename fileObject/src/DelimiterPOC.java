import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class DelimiterPOC {

	public void readOffSetInput(String line, String offsetStr){
		String[] offSets = offsetStr.split("-");
		String[] words = new String[offSets.length];
		
		int start = 0;
		int end = 0;
		int num = 0;
		int numOffSet = 0;
		for(String offSet : offSets ){
			numOffSet = Integer.parseInt(offSet);
			end = start + numOffSet;
			words[num++] = line.substring(start,end);
			start = start + numOffSet;
		}
		
		System.out.println("Original line:"+line);
		for (String word: words){
			System.out.print(word+" ");
		}
		System.out.println();
	}
	
	public void fieldValidated(String inputStr){
		inputStr = "aaa|bbb|234|ddd";
		String regEx = "(.*\\|.*\\|\\d+\\|.*)";
	
		if (  inputStr.matches(regEx)){
			System.out.println(" matches :"+inputStr);
		}
		
		StringBuilder sb = new StringBuilder(100);
		inputStr = "aaa|bbb||ddd";
		Pattern p = Pattern.compile(regEx);
		Matcher m = p.matcher(inputStr);
		if (! m.matches() ) {
			String[] fields = inputStr.split("\\|");
			sb.append(fields[0]);
			sb.append("|");
			sb.append(fields[1]);
			sb.append("|");
			sb.append("9999");
			sb.append("|");
			sb.append(fields[3]);
			
			System.out.println("new string :"+sb.toString());
		}	
	}
	
	public void fieldNumValidated(String inputStr, int fieldToValidate){
		inputStr = "aaa|bbb|xx|dd";
		fieldToValidate=3;
		
		String[] fields = inputStr.split("\\|");
		StringBuilder sb = new StringBuilder(fields.length);
		
		for(int i=0; i< fields.length;i++){
			if (i == fieldToValidate && ! fields[(fieldToValidate -1)].matches("-?\\dd+") ){
				sb.append("9999|");
			}
			sb.append(fields[i] + "|");
		}
		
		
	}
	public void readCtrlCharInput(String inputStr) {	
		String line;
		String cleanLine;
		try {
		List<String> lines = FileUtils.readLines(new File(inputStr));
		Iterator<String> itr = lines.iterator();
		while (itr.hasNext()){
			line =  itr.next();
			cleanLine = line.replaceAll("\\p{Cntrl}", "|");
			//cleanLine = line.replaceAll("[\u0000-\u001f]","|");
			//cleanLine = line.replace('\u0001','|'); 
			String[] words = line.split("\\p{Cntrl}");
			System.out.println("Original line:"+line);
			System.out.println("Replaced Ctrl char with '|' :"+cleanLine);	
			for(String word : words){
				System.out.print(word+" ");
			}
		}	
		} catch (IOException e) {
			e.printStackTrace();
		} 
	
	}
	
	public static void main(String[] args) {
		String offSetInput = "aaabbbbccdddfff";
		String offSetStr = "3-4-2-3-3";
		
		DelimiterPOC dPOC = new DelimiterPOC();
		//dPOC.readOffSetInput(offSetInput, offSetStr);
		
		String localStr="/home/training/sampleInput/case2.txt";
		dPOC.readCtrlCharInput(localStr);

		//dPOC.fieldValidated("aaa|bbb|123|ddd");
	}

}
