import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;


public class TestRun {

	/**
	 * Input data:
	 * "e": "view"
	 * "pv": "32233..232" 
	 */
	public static void main(String[] args) throws IOException {
		Map<String, String> jsonSet = new HashMap<String, String>();
		BufferedReader br = new BufferedReader(new FileReader("/home/training/ll/ad1.txt"),2000);

		String line = null;
		String eventType = null;
		String pvKey = null;
		int startIdx = 0;
		int endIdx = 0;
		while ( (line = br.readLine()) != null ){
			startIdx = line.indexOf("\"e\":")+5;
			endIdx = line.indexOf("\"",startIdx);
			eventType = line.substring(startIdx, endIdx);
			
			startIdx = line.indexOf("\"pv\":")+6;
			endIdx = line.indexOf("\"", startIdx);
			pvKey = line.substring(startIdx, endIdx);

		System.out.printf("startIdx : %d  endIdx : %d\n ", startIdx, endIdx);
		System.out.printf("eventtype : |%s| | pvKey : |%s|\n\n",eventType, pvKey);
		}
	}

}
