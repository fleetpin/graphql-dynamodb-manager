package com.fleetpin.graphql.database.manager;

public class RevisionMismatchException extends RuntimeException{

	public RevisionMismatchException(Throwable cause) {
		super(cause);
	}

}
