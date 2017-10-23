package com.macro.test.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtils {
	
	public static String pattern_default = "yyyy-MM-dd HH:mm:ss";
	public static String pattern_1 = "yyyy-MM-dd HH:mm";
	
	/**
	 * 判断某时间是否在某时间段内
	 * @param curr_time 需要判断的时间
	 * @param start_time 时段开始时间
	 * @param end_time 时段结束时间
	 * @return true|flase
	 */
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
	
	/**
	 * 判斷日期格式是否正確
	 * @param str
	 * @return true|false
	 */
	public static boolean isValidDate(String str) {
		boolean convertSuccess=true;
	    SimpleDateFormat format = new SimpleDateFormat(pattern_default);
	    try {
	    	// 设置lenient为false. 否则SimpleDateFormat会比较宽松地验证日期，比如2007/02/29会被接受，并转换成2007/03/01
	    	format.setLenient(false);
	    	format.parse(str);
	    } catch (ParseException e) {
	    	// e.printStackTrace();
	    	// 如果throw java.text.ParseException或者NullPointerException，就说明格式不对
	    	convertSuccess=false;
	    } 
	    return convertSuccess;
	}

	public static void main(String[] args) {

		//boolean b = isInTimePeriod("2015-11-30 13:59:59", "2015-11-30 12:00:00", "2015-11-30 13:59:59");
		//boolean b = isValidDate("2017-11-30 13:59");
	}

}
