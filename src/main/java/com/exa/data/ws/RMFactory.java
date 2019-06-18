package com.exa.data.ws;

import java.util.Map;

import com.exa.data.DynamicField;

public interface RMFactory {

	ResponseManager create(Map<String, DynamicField> fields, String path);
}
