package com.exa.data.sql.oracle;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.exa.data.sql.DataFormatter;

public class PLSQLDateFormatter extends DataFormatter<Date> {
	
	final static DateFormat DF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

	@Override
	public String toSQL(Date rawValue, String defaultValue) {
		if(rawValue == null) return defaultValue;
		
		return "TO_DATE('" + DF.format(rawValue) + "', 'YYYY-MM-DD HH24:MI:SS')";
	}

	@Override
	public String toSQLFromString(String str, String format) {
		if(format == null) format = "YYYY-MM-DD HH24:MI:SS";
		return "TO_DATE('" + str + "', '" + format + "')";
	}

}
