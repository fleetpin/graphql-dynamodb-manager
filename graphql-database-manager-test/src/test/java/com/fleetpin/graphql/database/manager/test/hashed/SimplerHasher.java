package com.fleetpin.graphql.database.manager.test.hashed;

import com.fleetpin.graphql.database.manager.annotations.Hash.HashExtractor;

public class SimplerHasher implements HashExtractor {

	@Override
	public String hashId(String id) {
		return id.substring(0, 4);
	}

	@Override
	public String sortId(String id) {
		return id.substring(4);
	}
}
