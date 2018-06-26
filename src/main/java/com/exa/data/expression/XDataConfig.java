package com.exa.data.expression;

import com.exa.expression.eval.ClassesMan;

public class XDataConfig {
	public static void setup(ClassesMan classesMan) {
		classesMan.registerClass(new TDataReader());
	}
}
