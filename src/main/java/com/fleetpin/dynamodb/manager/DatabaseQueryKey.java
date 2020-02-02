package com.fleetpin.dynamodb.manager;

import java.util.Objects;

public class DatabaseQueryKey {

	private final String organisationId;
	private final Class<? extends Table> type;

	DatabaseQueryKey(String organisationId, Class<? extends Table> type) {
		super();
		this.organisationId = organisationId;
		this.type = type;
	}

	public String getOrganisationId() {
		return organisationId;
	}

	public Class<? extends Table> getType() {
		return type;
	}

	@Override
	public int hashCode() {
		return Objects.hash(organisationId, type);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DatabaseQueryKey other = (DatabaseQueryKey) obj;
		return Objects.equals(organisationId, other.organisationId) && Objects.equals(type, other.type);
	}

}
