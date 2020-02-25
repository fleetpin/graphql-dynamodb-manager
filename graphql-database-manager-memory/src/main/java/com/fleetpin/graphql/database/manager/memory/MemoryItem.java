package com.fleetpin.graphql.database.manager.memory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fleetpin.graphql.database.manager.Table;
import com.google.common.collect.HashMultimap;

public class MemoryItem {

	private String organisationId;
	private String id;
	
	public MemoryItem(HashMultimap<String, String> links, Table entity) {
	}

	public static MemoryItem deleted() {
		// TODO Auto-generated method stub
		return null;
	}

	public void deleteLinks() {
		// TODO Auto-generated method stub
		
	}

	public void deleteLinksTo(MemoryItem item) {
	}

	public boolean isDeleted() {
		return false; 
	}

	public <T> T getEntity() {
		// TODO Auto-generated method stub
		return null;
	}
	
}
