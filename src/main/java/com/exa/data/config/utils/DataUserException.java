package com.exa.data.config.utils;

import com.exa.data.DataException;

public class DataUserException extends DataException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private String safeUserMessage;
	
	public DataUserException(String message, String safeUserMessage) {
		super(message);
		
		this.safeUserMessage = safeUserMessage;
	}

	public String getSafeUserMessage() {
		return safeUserMessage;
	}

	@Override
	public String getMessage() {
		
		return getClass() + ": " + safeUserMessage + ":" + super.getMessage();
	}
	
	

	
}
