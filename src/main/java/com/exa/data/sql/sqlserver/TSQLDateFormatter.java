package com.exa.data.sql.sqlserver;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.exa.data.sql.DataFormatter;

public class TSQLDateFormatter extends DataFormatter<Date> {
	
	final static DateFormat DF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	@Override
	public String toSQL(Date rawValue, String defaultValue) {
		if(rawValue == null) return defaultValue;
		
		return "CONVERT(datetime, '" + DF.format(rawValue) + "', 120)";
	}

	@Override
	public String toSQLFromString(String str, String format) {
		if(format == null) format = "120";
		
		return "CONVERT(datetime, '" + str + "'," + format + ")";
	}

}
