package com.macro.test.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtils {
	
	public static String pattern_default = "yyyy-MM-dd HH:mm:ss";
	
	public static boolean isInTimePeriod(String curr_time,String start_time,String end_time){
		Date cTime = null;
		Date sTime = null;
		Date eTime = null;
		try {
			cTime = new SimpleDateFormat(pattern_default).parse(curr_time);
			sTime = new SimpleDateFormat(pattern_default).parse(start_time);
			eTime = new SimpleDateFormat(pattern_default).parse(end_time);
			
			if(cTime.before(eTime) && cTime.after(sTime))
				return true;
		} catch (ParseException e) {
			//e.printStackTrace();
		}
		
		return false;
	}

	public static void main(String[] args) {

		boolean b = isInTimePeriod("2015-11-30 13:59:59", "2015-11-30 12:00:00", "2015-11-30 13:59:59");
		System.out.println(b);
	}

}
