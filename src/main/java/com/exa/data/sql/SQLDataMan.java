package com.exa.data.sql;

import com.exa.data.DataException;
import com.exa.data.DataMan;

public interface SQLDataMan extends DataMan {
	String getSQL() throws DataException;
}
