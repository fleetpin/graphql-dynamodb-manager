package com.fleetpin.graphql.dynamodb.manager;

import java.time.Instant;
import java.util.Collection;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fleetpin.graphql.builder.annotations.GraphQLIgnore;
import com.fleetpin.graphql.builder.annotations.Id;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public abstract class Table {

	private String id;
	private Instant createdAt;
	private Instant updatedAt;
	private String sourceTable;
	private String sourceOrganistaionId;
	private HashMultimap<String, String> links = HashMultimap.create();
	
	@Id
	public String getId() {
		return id;
	}
	
	public void setId(String id) {
		this.id = id;
	}
	
	final void setCreatedAt(Instant createdAt) {
		this.createdAt = createdAt;
	}
	final void setUpdatedAt(Instant updatedAt) {
		this.updatedAt = updatedAt;
	}
	public final Instant getCreatedAt() {
		return createdAt;
	}
	public final Instant getUpdatedAt() {
		return updatedAt;
	}

	@Override
	public int hashCode() {
		return Objects.hash(getId());
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if(obj instanceof Table) {
			Table other = (Table) obj;
			return Objects.equals(getId(), other.getId());
		}
		return false;
	}

	@JsonIgnore
	@GraphQLIgnore
	String getSourceTable() {
		return sourceTable;
	}
	void setSource(String sourceTable, HashMultimap<String, String> links, String sourceOrganisationId) {
		//so bad data does not cause error
		if(createdAt == null) {
			createdAt = Instant.MIN;
		}
		if(updatedAt == null) {
			updatedAt = Instant.MIN;
		}
		this.sourceTable = sourceTable;
		this.links = links;
		this.sourceOrganistaionId = sourceOrganisationId;
	}
	
	@JsonIgnore
	@GraphQLIgnore
	String getSourceOrganistaionId() {
		return sourceOrganistaionId;
	}

	void setLinks(String type, Collection<String> groupIds) {
		this.links.removeAll(type);
		this.links.putAll(type, groupIds);
	}
	
	@JsonIgnore
	@GraphQLIgnore
	HashMultimap<String, String> getLinks() {
		return links;
	}

}
