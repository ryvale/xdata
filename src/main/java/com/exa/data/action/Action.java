package com.exa.data.action;

import com.exa.utils.ManagedException;

public interface Action {
	void execute() throws ManagedException;
}
