import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;


public class DateFormatter {

	/**
	 * @param args
	 * @throws ParseException 
	 */
	public static void main(String[] args) throws ParseException {
		SimpleDateFormat inputFormatter1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		SimpleDateFormat inputFormatter2 = new SimpleDateFormat("yyyy-MM-dd");
		SimpleDateFormat outputFormatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss -05:00", Locale.US);
		
		String inputStr = "2014-05-20 20:32:22";
		Date parsedDate = inputFormatter1.parse(inputStr);
		System.out.println("Input Formatter 1 parse Non-UTC :"+ parsedDate.toString());
		inputFormatter1.setTimeZone(TimeZone.getTimeZone("UTC"));
		parsedDate = inputFormatter1.parse(inputStr);
		System.out.println("Input Formatter 1 parse UTC:"+ parsedDate.toString());
		
		outputFormatter.setTimeZone(TimeZone.getDefault());
		String dateStr = outputFormatter.format(parsedDate);
		System.out.println("Formatted of outputformatter Date default local timezone:"+dateStr);
	
		
		parsedDate = inputFormatter2.parse(inputStr);
		dateStr = outputFormatter.format(parsedDate);
		System.out.println("Input Formatter 2 parse :"+ parsedDate.toString());
		System.out.println("Formatted of input date:"+dateStr);

	}

}
