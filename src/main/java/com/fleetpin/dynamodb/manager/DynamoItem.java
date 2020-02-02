package com.fleetpin.dynamodb.manager;

import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public class DynamoItem implements Comparable<DynamoItem>{

	private final String table;
	private final Map<String, AttributeValue> item;
	private final String id;

	private final Multimap<String, String> links;

	DynamoItem(String table, Map<String, AttributeValue> item) {
		this.table = table;
		this.item = item;

		this.links = HashMultimap.create();

		var links = item.get("links");
		if(links != null) {
			links.m().forEach((t, value) -> {
				this.links.putAll(t, value.ss());
			});
		}
		String id = item.get("id").s();
		this.id = id.substring(id.indexOf(':') + 1);

	}

	public boolean isDeleted() {
		var deleted = item.get("deleted");
		if(deleted != null && deleted.bool()) {
			return true;
		}
		return false;
	}

	//TODO: AWS has made this more difficult with version 2 of the api keep an eye out might get easy again in the future
	public <T> T convertTo(ObjectMapper mapper, Class<T> type) {
		if(isDeleted()) {
			return null;
		}
		var table = TableUtil.convertTo(mapper, item.get("item"), type);
		if(table != null && table instanceof Table) {
			((Table) table).setSource(this.table, links, item.get("organisationId").s());
		}
		return table;
	}
	
	public String getTable() {
		return table;
	}
	
	Map<String, AttributeValue> getItem() {
		return item;
	}

	public Multimap<String, String> getLinks() {
		return links;
	}
	
	public String getId() {
		return id;
	}

	@Override
	public int compareTo(DynamoItem o) {
		return getId().compareTo(o.getId());
	}

	public String getField(String tableTarget) {
		var attribute = item.get(tableTarget);
		if(attribute == null) {
			return null;
		}
		return attribute.s();
	}



}
