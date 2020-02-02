package com.fleetpin.graphql.dynamodb.manager;

import java.util.Objects;

public class DatabaseKey {

	private final String organisationId;
	private final Class<? extends Table> type;
	private final String id;

	DatabaseKey(String organisationId, Class<? extends Table> type, String id) {
		this.organisationId = organisationId;
		this.type = type;
		this.id = id;
	}

	public String getOrganisationId() {
		return organisationId;
	}

	public Class<? extends Table> getType() {
		return type;
	}

	public String getId() {
		return id;
	}

	@Override
	public int hashCode() {
		return Objects.hash(id, organisationId, type);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DatabaseKey other = (DatabaseKey) obj;
		return Objects.equals(id, other.id) && Objects.equals(organisationId, other.organisationId)
				&& Objects.equals(type, other.type);
	}

	@Override
	public String toString() {
		return "DatabaseKey [organisationId=" + organisationId + ", type=" + type + ", id=" + id + "]";
	}
	
	
	
	

}
