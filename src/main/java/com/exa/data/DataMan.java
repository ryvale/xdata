package com.exa.data;


public interface DataMan {
	boolean execute() throws DataException;
	
	DataMan cloneDR() throws DataException;
}
