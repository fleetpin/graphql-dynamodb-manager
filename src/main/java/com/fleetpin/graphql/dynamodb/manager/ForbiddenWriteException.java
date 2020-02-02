package com.fleetpin.graphql.dynamodb.manager;

public class ForbiddenWriteException extends RuntimeException {
	public ForbiddenWriteException(String msg) {
		super(msg);
	}
}
